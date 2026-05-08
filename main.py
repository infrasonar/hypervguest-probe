from libprobe.probe import Probe
from lib.check.hypervguest import CheckHyperVGuest
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = (
        CheckHyperVGuest,
    )

    probe = Probe("hypervguest", version, checks, loggers=('aiowmi',))

    probe.start()
