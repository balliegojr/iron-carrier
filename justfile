default:
	just -l

install-bin:
	cargo build --release
	sudo cp ./target/release/iron-carrier /usr/bin/

install-as-user: install-bin
	systemctl stop --user iron-carrier.service
	sudo cp ./system.d/iron-carrier.service /usr/lib/systemd/user
	systemctl enable --user iron-carrier.service
	systemctl start --user iron-carrier.service

build-rasp:
    ~/.cargo/bin/cross build --release --target=armv7-unknown-linux-gnueabihf

