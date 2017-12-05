
class Clock:
    """
    Implement a Lamport clock
    """
    def __init__(self, initial_value=0):
        self.timer = initial_value


    def is_in_past(self, timer, update=True):
        if self.timer < timer:
            if update:
                self.update_clock(timer)
            return True
        return False


    def update_clock(self, timer):
        self.timer = max(self.timer, timer)