# Publish to GitHub

## 1. Create the repository locally

```bash
unzip fault-tolerant-sharded-kv-store.zip
cd fault-tolerant-sharded-kv-store
git init
git add .
git commit -m "Build fault-tolerant sharded key-value store"
git branch -M main
```

## 2. Create and push the GitHub repository

Using GitHub CLI:

```bash
gh auth login
gh repo create fault-tolerant-sharded-kv-store --public --source=. --remote=origin --push
```

Or create an empty repository in the GitHub website and run:

```bash
git remote add origin https://github.com/pavankumaretta/fault-tolerant-sharded-kv-store.git
git push -u origin main
```

## 3. Final proof-of-work checklist

- Replace the repository URL in your resume and portfolio.
- Open the Actions tab and confirm both CI jobs pass.
- Run `make demo` and record a short terminal demo for LinkedIn or the README.
- Import the collection in `postman/` and capture the CRUD and failover responses.
- Add a repository description: `C++ sharded key-value store with Raft replication, SQLite persistence, failover, rate limiting, circuit breakers, and Prometheus metrics.`
- Add topics: `cpp`, `raft`, `distributed-systems`, `sqlite`, `fault-tolerance`, `api-gateway`, `prometheus`, `cmake`.
