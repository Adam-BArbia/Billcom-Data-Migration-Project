import logging as log
import yaml

def configure_logging(config):
    level = getattr(log, config['logging']['level'].upper(), log.INFO)
    log.basicConfig(level=level, format=config['logging']['format'])


"""                   _          _          _          _          _
                    >(')____,  >(')____,  >(')____,  >(')____,  >(') ___,
                      (` =~~/    (` =~~/    (` =~~/    (` =~~/    (` =~~/
^~^~^~^~^~^~^~^~^~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^`---'~^~^~^~^~^~^~^~^~
"""
