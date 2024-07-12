import logging.config
import yaml

def configure_logging(config):
    with open(config['logging']['config_file'], 'r') as file:
        log_config = yaml.safe_load(file)
    logging.config.dictConfig(log_config)



#                      _          _          _          _          _
#                    >(')____,  >(')____,  >(')____,  >(')____,  >(') ___,
#                      (` =~~/    (` =~~/    (` =~~/    (` =~~/    (` =~~/
#^~^~^~^~^~^~^~^~^~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^~^~^~^~^~^~
#QUACKING THROUGH LIFE