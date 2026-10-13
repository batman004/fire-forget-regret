# Fire, Forget, Regret

Companion repository for the PyCon NL 2026 talk. It shows what breaks in a long-running pipeline, and how adding a
decorator makes it durable.

```
talk/     the live demo: the same pipeline twice, plain and durable
tests/    unittest for the demo
TALK.md   what the talk promised, what the repo covers, and the run sheet
```

The library behind the decorator, stitch, lives in its own repository, [batman004/stitch](https://github.com/batman004/stitch).
`requirements.txt` installs it from GitHub.

## The demo

A pipeline keeps searching for fresh AI news through a local OpenSERP server, downloads each new URL, asks a local
Ollama model (qwen3) whether it is a real breakthrough or just hype, stores the verdict in MongoDB and publishes the
result. Five workers, and it never stops unless `TALK_ARTICLES` caps it.

```bash
pip install -r requirements.txt        # stitch from GitHub, loguru, pymongo and openserp
ollama pull qwen3                      # and keep Ollama running
docker run -d -p 27017:27017 mongo:7   # or any MongoDB, see MONGO_URL
docker run -d -p 7000:7000 karust/openserp serve   # search; use 127.0.0.1, not localhost, on macOS

python -m talk.ai_news_pipeline reset
python -m talk.ai_news_pipeline run     # then kill -9 it, switch wifi off, or stop Ollama
python -m talk.ai_news_pipeline report  # what it published, duplicates, model calls, mongo documents

diff talk/ai_news_pipeline.py talk/ai_news_pipeline_durable.py     # everything durability adds
python -m talk.ai_news_pipeline_durable run       # same failures, now it resumes
python -m talk.ai_news_pipeline_durable heal      # replace fallback results once the model is back
```

To stress it, put a rate limit in front of Ollama: after every 1,000 requests the proxy answers 429 with a
`Retry-After` for 2 minutes. The pipeline scripts know nothing about it.

```bash
python -m talk.ollama_cooldown_proxy
OLLAMA_URL=http://localhost:11435/api/chat python -m talk.ai_news_pipeline_durable run
```

Knobs: `TALK_ARTICLES` (0 = forever), `TALK_WORKERS` (5), `TALK_RETRY_SECONDS` (1.0), `TALK_ENGINES` (duckduckgo,bing),
`OPENSERP_URL`, `OLLAMA_URL`, `OLLAMA_MODEL`, `MONGO_URL`;
for the proxy `TALK_COOLDOWN_EVERY` (1000), `TALK_COOLDOWN_SECONDS` (120), `TALK_PROXY_PORT` (11435).

The run sheet with what to expect in each act is in [TALK.md](TALK.md).

## Tests

```bash
python -m unittest
```

They cover both demo scripts (kill and resume, internet down, the model refusing, heal) and the Ollama cooldown proxy,
with a fake network. The demo tests are skipped if `loguru` or `pymongo` is not installed. stitch's own tests run in
its repository (`cd ../stitch && python -m unittest`). MIT licence.
