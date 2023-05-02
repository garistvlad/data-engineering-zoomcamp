import logging


def setup_logger(name: str) -> logging.Logger:
    """Predefined LOG  format: TIME - NAME - LEVEL - MESSAGE"""
    # logger
    logger = logging.getLogger(name=name)
    logger.setLevel(logging.INFO)
    # handler
    console_handler = logging.StreamHandler()
    # formatter
    formatter = logging.Formatter(
        fmt='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt="%Y-%m-%d %H:%M:%S"
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    return logger
