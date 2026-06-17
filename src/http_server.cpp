#include "kv/http_server.hpp"

#include "kv/common.hpp"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

#include <array>
#include <cerrno>
#include <cstring>
#include <sstream>
#include <stdexcept>
#include <thread>

namespace kv {
namespace {

bool send_all(int fd, const std::string& data) {
    std::size_t sent = 0;
    while (sent < data.size()) {
        const auto rc = ::send(fd, data.data() + sent, data.size() - sent, 0);
        if (rc <= 0) {
            return false;
        }
        sent += static_cast<std::size_t>(rc);
    }
    return true;
}

std::string trim(std::string value) {
    const auto first = value.find_first_not_of(" \t\r\n");
    if (first == std::string::npos) return "";
    const auto last = value.find_last_not_of(" \t\r\n");
    return value.substr(first, last - first + 1);
}

}  // namespace

HttpServer::HttpServer(std::string bind_address, std::uint16_t port, Handler handler)
    : bind_address_(std::move(bind_address)), port_(port), handler_(std::move(handler)) {}

HttpServer::~HttpServer() { stop(); }

void HttpServer::run() {
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0) {
        throw std::runtime_error("socket() failed: " + std::string(std::strerror(errno)));
    }

    int reuse = 1;
    ::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_port = htons(port_);
    if (::inet_pton(AF_INET, bind_address_.c_str(), &address.sin_addr) != 1) {
        ::close(server_fd_);
        server_fd_ = -1;
        throw std::runtime_error("invalid bind address: " + bind_address_);
    }

    if (::bind(server_fd_, reinterpret_cast<sockaddr*>(&address), sizeof(address)) < 0) {
        const std::string error = std::strerror(errno);
        ::close(server_fd_);
        server_fd_ = -1;
        throw std::runtime_error("bind() failed: " + error);
    }
    if (::listen(server_fd_, 128) < 0) {
        const std::string error = std::strerror(errno);
        ::close(server_fd_);
        server_fd_ = -1;
        throw std::runtime_error("listen() failed: " + error);
    }

    running_.store(true);
    log_event("INFO", "http_server_started", bind_address_ + ":" + std::to_string(port_));

    while (running_.load()) {
        sockaddr_in client_address{};
        socklen_t client_length = sizeof(client_address);
        const int client_fd = ::accept(server_fd_, reinterpret_cast<sockaddr*>(&client_address), &client_length);
        if (client_fd < 0) {
            if (!running_.load()) break;
            if (errno == EINTR) continue;
            log_event("ERROR", "accept_failed", std::strerror(errno));
            continue;
        }

        std::array<char, INET_ADDRSTRLEN> ip_buffer{};
        const char* ip = ::inet_ntop(AF_INET, &client_address.sin_addr, ip_buffer.data(), ip_buffer.size());
        std::thread(&HttpServer::handle_client, this, client_fd, ip ? ip : "unknown").detach();
    }
}

void HttpServer::stop() {
    if (!running_.exchange(false)) {
        return;
    }
    if (server_fd_ >= 0) {
        ::shutdown(server_fd_, SHUT_RDWR);
        ::close(server_fd_);
        server_fd_ = -1;
    }
}

void HttpServer::handle_client(int client_fd, const std::string& remote_ip) {
    timeval timeout{};
    timeout.tv_sec = 5;
    ::setsockopt(client_fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    ::setsockopt(client_fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));

    HttpResponse response;
    try {
        constexpr std::size_t max_request_size = 1024 * 1024 + 64 * 1024;
        std::string raw;
        std::array<char, 8192> buffer{};
        std::size_t header_end = std::string::npos;

        while ((header_end = raw.find("\r\n\r\n")) == std::string::npos) {
            const auto bytes = ::recv(client_fd, buffer.data(), buffer.size(), 0);
            if (bytes <= 0) {
                throw std::runtime_error("failed to read request headers");
            }
            raw.append(buffer.data(), static_cast<std::size_t>(bytes));
            if (raw.size() > max_request_size) {
                throw std::runtime_error("request too large");
            }
        }

        HttpRequest request;
        request.remote_ip = remote_ip;
        std::istringstream headers_stream(raw.substr(0, header_end));
        std::string request_line;
        std::getline(headers_stream, request_line);
        request_line = trim(request_line);
        {
            std::istringstream line(request_line);
            line >> request.method >> request.path >> request.version;
            if (request.method.empty() || request.path.empty()) {
                throw std::runtime_error("invalid request line");
            }
        }

        std::string header_line;
        std::size_t content_length = 0;
        while (std::getline(headers_stream, header_line)) {
            header_line = trim(header_line);
            if (header_line.empty()) continue;
            const auto colon = header_line.find(':');
            if (colon == std::string::npos) continue;
            auto name = to_lower(trim(header_line.substr(0, colon)));
            auto value = trim(header_line.substr(colon + 1));
            request.headers[name] = value;
            if (name == "content-length") {
                content_length = static_cast<std::size_t>(std::stoull(value));
            }
        }

        if (content_length > 1024 * 1024) {
            response.status = 413;
            response.body = "{\"error\":\"request body too large\"}";
        } else {
            request.body = raw.substr(header_end + 4);
            while (request.body.size() < content_length) {
                const auto remaining = content_length - request.body.size();
                const auto bytes = ::recv(client_fd, buffer.data(), std::min(buffer.size(), remaining), 0);
                if (bytes <= 0) {
                    throw std::runtime_error("incomplete request body");
                }
                request.body.append(buffer.data(), static_cast<std::size_t>(bytes));
            }
            if (request.body.size() > content_length) {
                request.body.resize(content_length);
            }
            response = handler_(request);
        }
    } catch (const std::exception& error) {
        response.status = 400;
        response.content_type = "application/json";
        response.body = "{\"error\":\"" + json_escape(error.what()) + "\"}";
    }

    std::ostringstream out;
    out << "HTTP/1.1 " << response.status << ' ' << reason_phrase(response.status) << "\r\n"
        << "Content-Type: " << response.content_type << "\r\n"
        << "Content-Length: " << response.body.size() << "\r\n"
        << "Connection: close\r\n";
    for (const auto& [name, value] : response.headers) {
        out << name << ": " << value << "\r\n";
    }
    out << "\r\n" << response.body;
    send_all(client_fd, out.str());
    ::shutdown(client_fd, SHUT_RDWR);
    ::close(client_fd);
}

std::string HttpServer::reason_phrase(int status) {
    switch (status) {
        case 200: return "OK";
        case 201: return "Created";
        case 400: return "Bad Request";
        case 401: return "Unauthorized";
        case 404: return "Not Found";
        case 405: return "Method Not Allowed";
        case 413: return "Payload Too Large";
        case 429: return "Too Many Requests";
        case 500: return "Internal Server Error";
        case 503: return "Service Unavailable";
        default: return "Response";
    }
}

}  // namespace kv
