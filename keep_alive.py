import os
from threading import Thread

from flask import Flask

app = Flask(__name__)


@app.get("/")
def home():
    return "Discord Bot is running!", 200


@app.get("/health")
def health():
    return {"status": "ok"}, 200


def run():
    # Render 會透過 PORT 環境變數指定 Web Service 監聽埠。
    port = int(os.getenv("PORT", "8080"))
    app.run(
        host="0.0.0.0",
        port=port,
        debug=False,
        use_reloader=False,
    )


def keep_alive():
    # daemon=True：Bot 結束時 Flask 執行緒也會一起結束。
    thread = Thread(target=run, name="keep-alive", daemon=True)
    thread.start()
    return thread
