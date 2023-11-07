from digitemp.device import TemperatureSensor
from digitemp.master import UART_Adapter

USB_SENSOR = '/dev/ttyUSB0'


def main():
    sensor = TemperatureSensor(UART_Adapter(USB_SENSOR))
    sensor.info()
    print(sensor.get_temperature())


if __name__ == '__main__':
    main()
