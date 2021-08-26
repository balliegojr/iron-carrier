build:
	cargo build --release

install: build config
    # DEF_SERVICE=$(pkg-config systemd --variable=systemduserunitdir)
	sudo cp ./target/release/iron-carrier /usr/bin/
	sudo cp ./system.d/iron-carrier.service /usr/lib/systemd/user
	systemctl enable --user iron-carrier.service
	systemctl start --user iron-carrier.service

config:
	mkdir -p ~/.config/iron_carrier
	echo "[paths]" > ~/.config/iron_carrier/config.toml
