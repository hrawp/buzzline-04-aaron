"""
json_consumer_aaron.py

Consume json messages from a Kafka topic and visualize keyword counts in real-time.

JSON is a set of key:value pairs. 

Example serialized Kafka message
"{\"message\": \"I love Python!\", \"author\": \"Eve\"}"

Example JSON message (after deserialization) to be analyzed
{"message": "I love Python!", "author": "Eve"}

"""

#####################################
# Import Modules
#####################################

# Import packages from Python Standard Library
import os
import json  # handle JSON parsing
from collections import defaultdict  # data structure for counting keyword occurrences
from matplotlib import cm
from matplotlib.colors import Normalize

# Import external packages
from dotenv import load_dotenv

# IMPORTANT
# Import Matplotlib.pyplot for live plotting
# Use the common alias 'plt' for Matplotlib.pyplot
# Know pyplot well
import matplotlib.pyplot as plt

# Import functions from local modules
from utils.utils_consumer import create_kafka_consumer
from utils.utils_logger import logger

#####################################
# Load Environment Variables
#####################################

load_dotenv()

#####################################
# Getter Functions for .env Variables
#####################################


def get_kafka_topic() -> str:
    """Fetch Kafka topic from environment or use default."""
    topic = os.getenv("PROJECT_TOPIC", "buzzline-topic")
    logger.info(f"Kafka topic: {topic}")
    return topic


def get_kafka_consumer_group_id() -> str:
    """Fetch Kafka consumer group id from environment or use default."""
    group_id: str = os.getenv("BUZZ_CONSUMER_GROUP_ID", "default_group")
    logger.info(f"Kafka consumer group id: {group_id}")
    return group_id


#####################################
# Set up data structures
#####################################

# Track how many times each keyword has appeared
keyword_counts = defaultdict(int)

# Track running average of sentiment for each keyword
keyword_sentiment_avg = defaultdict(float)

#####################################
# Set up live visuals
#####################################

# Use the subplots() method to create a tuple containing
# two objects at once:
# - a figure (which can have many axis)
# - an axis (what they call a chart in Matplotlib)
fig, ax = plt.subplots()

# Use the ion() method (stands for "interactive on")
# to turn on interactive mode for live updates
plt.ion()

#####################################
# Define an update chart function for live plotting
# This will get called every time a new message is processed
#####################################


def update_chart():
    """Update the live chart with the top 5 keyword sentiment averages."""
    ax.clear()

    # Get top 5 keywords by count
    top_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    keywords_list = [keyword for keyword, _ in top_keywords]

    # Convert sentiment to percentages (0–100)
    sentiment_avg_list = [keyword_sentiment_avg[keyword] * 100 for keyword in keywords_list]

    # Create a colormap (dark blue to light blue)
    cmap = cm.get_cmap("Blues")
    norm = Normalize(vmin=30, vmax=80)  # Map 30–80% to full gradient

    # Map each sentiment to a color in the gradient
    bar_colors = [cmap(norm(value)) for value in sentiment_avg_list]

    # Create the bar chart with colored bars
    bars = ax.bar(range(len(keywords_list)), sentiment_avg_list, color=bar_colors)

    # Set labels and title
    ax.set_xlabel("Keywords")
    ax.set_ylabel("Sentiment Percentage")
    ax.set_title("Top 5 Keywords by Count with Sentiment Rating")

    # Set x-ticks and labels
    ax.set_xticks(range(len(keywords_list)))
    ax.set_xticklabels(keywords_list, rotation=45, ha="right")

    # Format y-axis as percent
    from matplotlib.ticker import FuncFormatter
    ax.yaxis.set_major_formatter(FuncFormatter(lambda x, _: f"{int(x)}%"))

    # Add percentage labels above each bar
    for bar in bars:
        height = bar.get_height()
        ax.text(
            bar.get_x() + bar.get_width() / 2,
            height + 1,
            f"{height:.1f}%",
            ha='center',
            va='bottom',
            fontsize=9,
            color='black'
        )

    # Adjust layout and render
    plt.tight_layout()
    plt.draw()
    plt.pause(0.01)



#####################################
# Function to process a single message
# #####################################


def process_message(message: str) -> None:
    """
    Process a single JSON message from Kafka and update the chart.

    Args:
        message (str): The JSON message as a string.
    """
    try:
        # Log the raw message for debugging
        logger.debug(f"Raw message: {message}")

        # Parse the JSON string into a Python dictionary
        message_dict: dict = json.loads(message)

        # Ensure the processed JSON is logged for debugging
        logger.info(f"Processed JSON message: {message_dict}")

        if isinstance(message_dict, dict):
            # Extract the 'keyword' and 'sentiment' fields
            keyword = message_dict.get("keyword_mentioned", "unknown")
            sentiment_raw = message_dict.get("sentiment", None)

            # Validate and convert sentiment
            try:
                sentiment = float(sentiment_raw)
            except (TypeError, ValueError):
                logger.warning(f"Ignoring message due to invalid sentiment: {sentiment_raw}")
                return  # skip processing this message

            logger.info(f"Message received - Keyword: {keyword}, Sentiment: {sentiment}")

            # Increment the count for the keyword
            keyword_counts[keyword] += 1
            count = keyword_counts[keyword]

            # Calculate running average
            prev_avg = keyword_sentiment_avg[keyword]
            new_avg = prev_avg + (sentiment - prev_avg) / count

            # Update sentiment average
            keyword_sentiment_avg[keyword] = new_avg

            # Log the updated values
            formatted_avg = {k: round(v, 2) for k, v in keyword_sentiment_avg.items()}
            logger.info(f"Updated keyword sentiment averages: {formatted_avg}")
            logger.info(f"Updated keyword counts: {dict(keyword_counts)}")

            # Update the chart
            update_chart()
            logger.info(f"Chart updated successfully for keyword: {keyword}")

        else:
            logger.error(f"Expected a dictionary but got: {type(message_dict)}")

    except json.JSONDecodeError:
        logger.error(f"Invalid JSON message: {message}")
    except Exception as e:
        logger.error(f"Error processing message: {e}")


#####################################
# Define main function for this module
#####################################


def main() -> None:
    """
    Main entry point for the consumer.

    - Reads the Kafka topic name and consumer group ID from environment variables.
    - Creates a Kafka consumer using the `create_kafka_consumer` utility.
    - Polls messages and updates a live chart.
    """
    logger.info("START consumer.")

    # fetch .env content
    topic = get_kafka_topic()
    group_id = get_kafka_consumer_group_id()
    logger.info(f"Consumer: Topic '{topic}' and group '{group_id}'...")

    # Create the Kafka consumer using the helpful utility function.
    consumer = create_kafka_consumer(topic, group_id)

    # Poll and process messages
    logger.info(f"Polling messages from topic '{topic}'...")
    try:
        for message in consumer:
            # message is a complex object with metadata and value
            # Use the value attribute to extract the message as a string
            message_str = message.value
            logger.debug(f"Received message at offset {message.offset}: {message_str}")
            process_message(message_str)
    except KeyboardInterrupt:
        logger.warning("Consumer interrupted by user.")
    except Exception as e:
        logger.error(f"Error while consuming messages: {e}")
    finally:
        consumer.close()
        logger.info(f"Kafka consumer for topic '{topic}' closed.")

    logger.info(f"END consumer for topic '{topic}' and group '{group_id}'.")


#####################################
# Conditional Execution
#####################################

if __name__ == "__main__":

    # Call the main function to start the consumer
    main()

    # Turn off interactive mode after completion
    plt.ioff()  

    # Display the final chart
    plt.show()
