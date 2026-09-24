from dotenv import load_dotenv
import argparse

from src.consumers.router import consume_events
from src.producers.inference import publish_inference_events
from src.producers.persist import publish_random_events


load_dotenv()

# CLOCK HELPERS
# utc_now() → written into Kafka.
# event_time() → read back (JSON first, Kafka timestamp if old messages have no field).


def main() -> None:
        
    parser = argparse.ArgumentParser()
    parser.add_argument(
    "-r", "--random",
    action="store_true",
    help="Publish random events in a loop (sometimes repeats ids to try idempotency)",
    )
    parser.add_argument(
    "--inference",
    action="store_true",
    help="Publish inference (quote) requests in a loop",
    )
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("-p", "--publish", action="store_true",
                      help="Only publish events")
    mode.add_argument("-c", "--consume", action="store_true",
                      help="Only consume events")
    args = parser.parse_args()

    if args.inference:
        try:
            publish_inference_events()
        except KeyboardInterrupt:
            print("\n✓ Stopped producing inference events")
        return


    if args.random or args.publish:
        try:
            publish_random_events()
        except KeyboardInterrupt:
            print("\n✓ Stopped producing")
        return
    
    if args.consume:
        consume_events()
        return
    
    parser.print_help()


if __name__ == "__main__":
    main()