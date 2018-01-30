class Switch(object):
    """The INVOKER class"""
    @classmethod
    def execute(cls, command):
        command.execute()

class Command(object):
    """The COMMAND interface"""
    def __init__(self, obj):
        self._obj = obj

    def execute(self):
        raise NotImplementedError

class TurnOnCommand(Command):
    """The COMMAND for turning on the light"""
    def execute(self):
        self._obj.turn_on()

class TurnOffCommand(Command):
    """The COMMAND for turning off the light"""
    def execute(self):
        self._obj.turn_off()

class Light(object):
    """The RECEIVER class"""
    def turn_on(self):
        print("The light is on")

    def turn_off(self):
        print("The light is off")

class LightSwitchClient(object):
    """The CLIENT class"""
    def __init__(self):
        self._lamp = Light()
        self._switch = Switch()

    def switch(self, cmd):
        cmd = cmd.strip().upper()
        if cmd == "ON":
            Switch.execute(TurnOnCommand(self._lamp))
        elif cmd == "OFF":
            Switch.execute(TurnOffCommand(self._lamp))
        else:
            print("Argument 'ON' or 'OFF' is required.")

# Execute if this file is run as a script and not imported as a module
#if __name__ == "__main__":
#    light_switch = LightSwitchClient()
#    print("Switch ON test.")
#    light_switch.switch("ON")
#    print("Switch OFF test.")
#    light_switch.switch("OFF")
#    print("Invalid Command test.")
#    light_switch.switch("****")