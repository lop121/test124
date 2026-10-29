"""Локальный получатель с проверкой подписи и управляемыми ошибками для E2E."""

import hashlib
import hmac
import json
import os
import time
from http.server import BaseHTTPRequestHandler, HTTPServer

deliveries: dict[str, list[dict]] = {}


class Receiver(BaseHTTPRequestHandler):
    """Принимает уведомления, не сохраняя секреты или полное тело в журнале."""

    def log_message(self, format: str, *args) -> None:
        """Подавляет стандартный журнал адресов с параметрами webhook."""

    def send_json(self, status: int, payload: dict | list) -> None:
        """Возвращает ограниченный диагностический JSON для локальной проверки."""
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        """Показывает записи тестового получателя без публикации его порта наружу."""
        self.send_json(200, deliveries)

    def do_POST(self) -> None:
        """Проверяет подпись и эмулирует успех, временную либо постоянную ошибку."""
        body = self.rfile.read(int(self.headers.get("Content-Length", "0")))
        signature = hmac.new(
            os.environ["WEBHOOK_SECRET"].encode(), body, hashlib.sha256
        ).hexdigest()
        if not hmac.compare_digest(
            self.headers.get("X-Webhook-Signature", ""), f"sha256={signature}"
        ):
            self.send_json(401, {"detail": "Invalid signature"})
            return
        payment_id = self.headers.get("Idempotency-Key", "")
        attempts = deliveries.setdefault(payment_id, [])
        attempts.append({"body": json.loads(body), "path": self.path, "received_at": time.time()})
        if self.path == "/fail" or (self.path == "/flaky" and len(attempts) < 3):
            self.send_json(503, {"detail": "Simulated delivery failure"})
        elif self.path == "/redirect":
            self.send_response(302)
            self.send_header("Location", "http://demo-webhook:8080/success")
            self.send_header("Content-Length", "0")
            self.end_headers()
        else:
            print(
                json.dumps({"event": "demo_webhook_received", "payment_id": payment_id}), flush=True
            )
            self.send_json(200, {"received": True})


if __name__ == "__main__":
    HTTPServer(("0.0.0.0", 8080), Receiver).serve_forever()
