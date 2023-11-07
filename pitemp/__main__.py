from digitemp.device import TemperatureSensor
from digitemp.master import UART_Adapter

USB_SENSOR = '/dev/ttyUSB0'


def main():
    sensor = TemperatureSensor(UART_Adapter(USB_SENSOR))
    sensor.info()
    temperature_c = sensor.get_temperature()
    temperature_f = _convert_c_to_f(temperature_c)
    print(f"Temperature in F: {temperature_f}")


def _convert_c_to_f(c: float) -> float:
    return c * 9.0 / 5.0 + 32


if __name__ == '__main__':
    main()
