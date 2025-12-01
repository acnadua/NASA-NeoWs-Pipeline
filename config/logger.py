import logging
import colorlog

# Define color scheme
log_colors = {
    "DEBUG": "blue",
    "INFO": "green",
    "WARNING": "yellow",
    "ERROR": "red",
    "CRITICAL": "bold_red",
}

# Define log format
log_format = "%(asctime)s - [%(levelname)s] - %(module)s.py - %(message)s"

# Create color formatter
color_formatter = colorlog.ColoredFormatter(
    "%(log_color)s" + log_format, log_colors=log_colors
)

# Create file formatter (no color)
file_formatter = logging.Formatter(log_format)

# Create logger
logger = logging.getLogger("app_logger")
logger.setLevel(logging.DEBUG)  # Capture all levels
logger.propagate = False  # Prevent propagation to the root logger

# Only add handlers if they haven't been added already
if not logger.handlers:
    # Configure console handler
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(color_formatter)

    # Configure file handler
    file_handler = logging.FileHandler("app.log")
    file_handler.setFormatter(file_formatter)

    # Add handlers
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
