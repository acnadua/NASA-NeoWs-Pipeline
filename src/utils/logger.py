import logging

class _ColoredFormatter(logging.Formatter):
	COLORS = {
		'DEBUG': '\033[94m',    # Blue
		'INFO': '\033[92m',     # Green
		'WARNING': '\033[93m',  # Yellow
		'ERROR': '\033[91m',    # Red
		'RESET': '\033[0m',
	}

	def format(self, record):
		color = self.COLORS.get(record.levelname, self.COLORS['RESET'])
		reset = self.COLORS['RESET']
		message = super().format(record)
		return f"{color}{message}{reset}"

def _get_logger(name: str = "job_scraper"):
	logger = logging.getLogger(name)
	if not logger.hasHandlers():
		handler = logging.StreamHandler()
		formatter = _ColoredFormatter('[%(levelname)s] %(asctime)s - %(message)s', "%Y-%m-%d %H:%M:%S")
		handler.setFormatter(formatter)
		logger.addHandler(handler)
	logger.setLevel(logging.DEBUG)
	return logger

logger = _get_logger()
