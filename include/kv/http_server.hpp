#pragma once

#include <atomic>
#include <cstdint>
#include <functional>
#include <map>
#include <string>

namespace kv {

struct HttpRequest {
    std::string method;
    std::string path;
    std::string version;
    std::map<std::string, std::string> headers;
    std::string body;
    std::string remote_ip;
};

struct HttpResponse {
    int status{200};
    std::string content_type{"application/json"};
    std::string body;
    std::map<std::string, std::string> headers;
};

class HttpServer {
public:
    using Handler = std::function<HttpResponse(const HttpRequest&)>;

    HttpServer(std::string bind_address, std::uint16_t port, Handler handler);
    ~HttpServer();

    HttpServer(const HttpServer&) = delete;
    HttpServer& operator=(const HttpServer&) = delete;

    void run();
    void stop();

private:
    void handle_client(int client_fd, const std::string& remote_ip);
    static std::string reason_phrase(int status);

    std::string bind_address_;
    std::uint16_t port_;
    Handler handler_;
    std::atomic<bool> running_{false};
    int server_fd_{-1};
};

}  // namespace kv
