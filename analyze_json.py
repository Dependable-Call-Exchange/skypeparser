#!/usr/bin/env python3
"""
Script to analyze the structure of the Skype export JSON file.
"""
import json
import sys


def analyze_json(file_path):
    """Analyze the structure of a JSON file."""
    try:
        with open(file_path, "r") as f:
            data = json.load(f)

        print("Top-level keys:", list(data.keys()))

        if "conversations" in data:
            conversations = data["conversations"]
            print(f"Number of conversations: {len(conversations)}")

            if conversations:
                first_conv = conversations[0]
                print(f"First conversation keys: {list(first_conv.keys())}")

                if "MessageList" in first_conv:
                    messages = first_conv["MessageList"]
                    print(f"Number of messages in first conversation: {len(messages)}")

                    if messages:
                        print(f"First message keys: {list(messages[0].keys())}")

                        # Print a sample message with content
                        for msg in messages[:10]:
                            if "content" in msg and msg["content"]:
                                print("\nSample message content:")
                                print(
                                    f"Message type: {msg.get('messagetype', 'Unknown')}"
                                )
                                print(f"Content: {msg['content'][:200]}...")
                                break

    except Exception as e:
        print(f"Error analyzing JSON: {e}")
        return 1

    return 0


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python analyze_json.py <json_file_path>")
        sys.exit(1)

    sys.exit(analyze_json(sys.argv[1]))
