import logging
from time import perf_counter
class ProcessTimer:
    def __init__(self, name, logger=None):
        self.name = name
        self.logger = logger if logger is not None else logging.getLogger(__name__)
        self.started = self.previous = perf_counter()

    def step(self, label):        
        now = perf_counter()
        self.logger.info("Timing | %s | %s | %.3f seconds",
                         self.name, label, now - self.previous)
        self.previous = now

    def total(self):        
        self.logger.info("Timing | %s | total | %.3f seconds",
                         self.name, perf_counter() - self.started)
