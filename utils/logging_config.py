import logging


def setup_logging() -> logging.Logger:
    logging_format = "[%(asctime)s: %(levelname)s: [%(module)s]: %(message)s]"
    logging.basicConfig(
        level=logging.INFO,
        format=logging_format,
    )
    logger = logging.getLogger("stock_ETL")

    # Suppress Great Expectations doc-decorator noise
    gx_logger = logging.getLogger("great_expectations._docs_decorators")
    gx_logger.setLevel(logging.WARNING)

    return logger
