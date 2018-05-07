# strowger-event-relayer
Stowger Event Relayer

### Building

```bash
sudo apt-get install librdkafka-dev -t stretch-backports
make
```

### Installing

```bash
sudo dpkg -i strowger-event-relayer.deb #if build locally
sudo systemctl start  strowger-relayer
```