class Comparison:
    def __init__(self, old_value, new_value, hysteresis, over_high_value, high_value, low_value, under_low_value, status, old_status):
        self.old_value = old_value
        self.new_value = new_value
        self.hysteresis = hysteresis
        self.over_high_value = over_high_value
        self.high_value = high_value
        self.low_value = low_value
        self.under_low_value = under_low_value
        self.status = status
        self.old_status = old_status
    def compare(self):
        if self.new_value > self.old_value:
            if self.status == 1:
                if self.new_value > self.over_high_value + self.hysteresis:
                    self.status = 5
                elif self.new_value > self.high_value + self.hysteresis:
                    self.status = 4
                elif self.new_value > self.low_value + self.hysteresis:
                    self.status = 3
                elif self.new_value > self.under_low_value + self.hysteresis:
                    self.status = 2
            elif self.status == 2:
                if self.new_value > self.over_high_value + self.hysteresis:
                    self.status = 5
                elif self.new_value > self.high_value + self.hysteresis:
                    self.status = 4
                elif self.new_value > self.low_value + self.hysteresis:
                    self.status = 3
            elif self.status == 3:
                if self.new_value > self.over_high_value + self.hysteresis:
                    self.status = 5
                elif self.new_value > self.high_value + self.hysteresis:
                    self.status = 4
            elif self.status == 4:
                if self.new_value > self.over_high_value + self.hysteresis:
                    self.status = 5

        elif self.new_value < self.old_value:
            if self.status == 5:
                if self.new_value < self.under_low_value - self.hysteresis:
                    self.status = 1
                elif self.new_value < self.low_value - self.hysteresis:
                    self.status = 2
                elif self.new_value < self.high_value - self.hysteresis:
                    self.status = 3
                elif self.new_value < self.over_high_value - self.hysteresis:
                    self.status = 4
            elif self.status == 4:
                if self.new_value < self.under_low_value - self.hysteresis:
                    self.status = 1
                elif self.new_value < self.low_value - self.hysteresis:
                    self.status = 2
                elif self.new_value < self.high_value - self.hysteresis:
                    self.status = 3
            elif self.status == 3:
                if self.new_value < self.under_low_value - self.hysteresis:
                    self.status = 1
                elif self.new_value < self.low_value - self.hysteresis:
                    self.status = 2
            elif self.status == 2:
                if self.new_value < self.under_low_value - self.hysteresis:
                    self.status = 1
        return self.status
    def event(self):
        if self.old_status != self.status:
            return "1"
        else:
            return "0"



# over_high_value = 100
# high_value = 80
# low_value = 40
# under_low_value = 20

# compare_values = Comparison(50, 60, 2, over_high_value, high_value, low_value, under_low_value, status=3, old_status=3)
# print(compare_values.compare())
# print(compare_values.event())


