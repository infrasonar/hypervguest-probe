from libprobe.probe import Probe
from lib.check.hypervguest import check_hypervguest
from lib.version import __version__ as version


if __name__ == '__main__':
    checks = {
        'hypervguest': check_hypervguest
    }

    probe = Probe("hypervguest", version, checks)

    probe.start()
