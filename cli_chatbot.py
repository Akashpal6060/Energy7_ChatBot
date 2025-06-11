# cli_chatbot.py

from core.chatbot_core import chatbot_answer
import logging, sys
logging.basicConfig(stream=sys.stderr, level=logging.DEBUG, force=True)

def run_cli():
    print("────────────────────────────────────────")
    print("  Welcome to your HF-powered DB Chatbot  ")
    print("     (type 'exit' or 'quit' to stop)    ")
    print("────────────────────────────────────────\n")

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye!")
            break

        if not question:
            continue
        if question.lower() in ("exit", "quit"):
            print("Goodbye!")
            break

        answer = chatbot_answer(question)
        print("\nBot:", answer, "\n")

if __name__ == "__main__":
    run_cli()
