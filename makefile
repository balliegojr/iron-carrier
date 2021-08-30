build:
	cargo build --release

install: build
    # DEF_SERVICE=$(pkg-config systemd --variable=systemduserunitdir)
	systemctl stop --user iron-carrier.service
	sudo cp ./target/release/iron-carrier /usr/bin/
	sudo cp ./system.d/iron-carrier.service /usr/lib/systemd/user
	systemctl enable --user iron-carrier.service
	systemctl start --user iron-carrier.service

config:
	mkdir -p ~/.config/iron-carrier
	echo "[paths]" > ~/.config/iron-carrier/config.toml
