import os
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import requests


class DoubaoConfigError(Exception):
    pass


class DoubaoRequestError(Exception):
    pass


@dataclass
class DoubaoConfig:
    api_key: str
    model: str
    base_url: str = "https://ark.cn-beijing.volces.com/api/v3"
    timeout: int = 60

    @classmethod
    def from_env(cls) -> "DoubaoConfig":
        api_key = os.getenv("DOUBAO_API_KEY", "").strip()
        model = os.getenv("DOUBAO_MODEL", "").strip()
        base_url = os.getenv("DOUBAO_BASE_URL", "https://ark.cn-beijing.volces.com/api/v3").strip()
        timeout_raw = os.getenv("DOUBAO_TIMEOUT", "60").strip()

        if not api_key:
            raise DoubaoConfigError("未配置 DOUBAO_API_KEY。")
        if not model:
            raise DoubaoConfigError("未配置 DOUBAO_MODEL。")

        try:
            timeout = int(timeout_raw)
        except ValueError as exc:
            raise DoubaoConfigError("DOUBAO_TIMEOUT 必须是整数。") from exc

        return cls(api_key=api_key, model=model, base_url=base_url.rstrip("/"), timeout=timeout)


def is_doubao_configured() -> bool:
    return bool(os.getenv("DOUBAO_API_KEY", "").strip() and os.getenv("DOUBAO_MODEL", "").strip())


class DoubaoClient:
    def __init__(self, config: Optional[DoubaoConfig] = None):
        self.config = config or DoubaoConfig.from_env()

    def chat(
        self,
        messages: List[Dict[str, str]],
        temperature: float = 0.3,
        max_tokens: int = 1200,
    ) -> str:
        url = f"{self.config.base_url}/chat/completions"
        headers = {
            "Authorization": f"Bearer {self.config.api_key}",
            "Content-Type": "application/json",
        }
        payload: Dict[str, Any] = {
            "model": self.config.model,
            "messages": messages,
            "temperature": temperature,
            "max_tokens": max_tokens,
        }

        try:
            response = requests.post(url, headers=headers, json=payload, timeout=self.config.timeout)
            response.raise_for_status()
            body = response.json()
        except requests.RequestException as exc:
            raise DoubaoRequestError(f"豆包请求失败: {exc}") from exc
        except ValueError as exc:
            raise DoubaoRequestError("豆包返回了无法解析的 JSON。") from exc

        choices = body.get("choices")
        if not isinstance(choices, list) or not choices:
            raise DoubaoRequestError("豆包返回结果为空。")

        first_choice = choices[0]
        if not isinstance(first_choice, dict):
            raise DoubaoRequestError("豆包返回格式异常。")

        message = first_choice.get("message")
        if not isinstance(message, dict):
            raise DoubaoRequestError("豆包返回消息格式异常。")

        content = message.get("content")
        if isinstance(content, str) and content.strip():
            return content.strip()

        if isinstance(content, list):
            text_parts = []
            for item in content:
                if isinstance(item, dict) and item.get("type") == "text":
                    text = item.get("text")
                    if isinstance(text, str):
                        text_parts.append(text)
            if text_parts:
                return "\n".join(text_parts).strip()

        raise DoubaoRequestError("豆包未返回可用文本内容。")
