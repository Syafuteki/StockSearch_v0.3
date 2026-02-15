from jpswing.intel.llm_client import IntelLlmClient


def test_extract_content_handles_gpt_oss_control_prefix() -> None:
    response = {
        "choices": [
            {
                "message": {
                    "content": '<|channel|>final <|constrain|>json<|message|>{"headline":"h","published_at":null}'
                }
            }
        ]
    }
    content = IntelLlmClient._extract_content(response)
    assert content.startswith('{"headline":"h"')


def test_extract_content_handles_lmstudio_chat_output() -> None:
    response = {
        "output": [
            {"type": "reasoning", "text": "thinking"},
            {
                "type": "message",
                "content": [{"type": "text", "text": '```json\n{"headline":"h2","published_at":null}\n```'}],
            },
        ]
    }
    content = IntelLlmClient._extract_content(response)
    assert content == '{"headline":"h2","published_at":null}'


def test_resolve_mcp_chat_endpoint_from_base_url() -> None:
    client = IntelLlmClient(base_url="http://host.docker.internal:1234/v1", model="openai/gpt-oss-20b")
    assert client._resolve_mcp_chat_endpoint() == "http://host.docker.internal:1234/api/v1/chat"
