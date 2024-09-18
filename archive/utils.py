
#### Logging using dictconfig
# def setup_logging(config_file_path) -> None:
#     config_file = pathlib.Path(config_file_path)
#     with open(config_file) as f:
#         config = json.load(f)
#     logging.config.dictConfig(config)
#     return

# #Refactor logging functionality
# def set_logger(name = "default") -> logging.Logger:
#     return logging.getLogger(name)


#Logging with root
#Defining logging level
# def set_logger() -> None:
#     root = logging.getLogger()
#     root.setLevel(logging.INFO)

#     handler = logging.StreamHandler(sys.stdout)
#     handler.setLevel(logging.INFO)
#     formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s',datefmt = "%Y-%m-%dT%H:%M:%S%z")
#     handler.setFormatter(formatter)
#     root.addHandler(handler)
#     return