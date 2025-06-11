"""
Simple “chat-completion” wrapper around Hugging Face Inference API.
Used for general conversation that doesn’t involve SQL.
"""

import os
from huggingface_hub import InferenceClient

# ── Import InferenceServerError in a version-safe way ───────────────
try:                                # ≥ 0.18 official location
    from huggingface_hub.inference._errors import InferenceServerError
except ModuleNotFoundError:
    try:                            # some versions re-export at top level
        from huggingface_hub import InferenceServerError
    except ImportError:
        try:                        # older releases (≤0.14)
            from huggingface_hub.utils._errors import InferenceServerError
        except ImportError:         # very old – create a shim
            class InferenceServerError(Exception):
                """Local shim for extremely old huggingface_hub versions."""
# ────────────────────────────────────────────────────────────────────

HF_TOKEN = os.getenv("HF_TOKEN")                       # set in .env
MODEL_ID = os.getenv("CHAT_MODEL",
                     "HuggingFaceH4/zephyr-7b-beta")   # any chat model

client = InferenceClient(model=MODEL_ID, token=HF_TOKEN)


def _extract(rsp) -> str:
    """Handle both new (str) and old (obj.generated_text) return types."""
    return rsp if isinstance(rsp, str) else rsp.generated_text


def chat_completion(prompt: str,
                    max_new_tokens: int = 256,
                    temperature: float = 0.7,
                    top_p: float = 0.9) -> str:
    """
    Generate a chat reply. Automatically falls back to the ‘conversational’
    endpoint if the selected model is only exposed under that task.
    """
    try:
        rsp = client.text_generation(
            prompt=prompt,
            max_new_tokens=max_new_tokens,
            temperature=temperature,
            top_p=top_p,
            stop=["User:"],     # new param name; works on all API versions
        )
        return _extract(rsp).strip()

    except InferenceServerError as err:
        # Free API exposes some models (e.g. Mixtral-Instruct) only via
        # the conversational task.  Detect and retry.
        if "Supported task: conversational" in str(err):
            rsp = client.conversational(
                inputs=prompt,
                parameters={
                    "max_new_tokens": max_new_tokens,
                    "temperature": temperature,
                    "top_p": top_p,
                    "stop": ["User:"],
                },
            )
            return _extract(rsp).strip()
        # For every other HF error, re-raise so the caller can handle it
        raise
