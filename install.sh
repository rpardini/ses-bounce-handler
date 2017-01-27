#! /bin/bash

# Horrible script for installation on Ubuntu machines

RELEASE=$(lsb_release -c -s)

if [ "$RELEASE" == "xenial" ]; then
	echo "Installing for xenial, which uses PHP 7..."
	apt-get -y install php-mbstring php-pear php-cli php-dev libssl-dev libmongo-client-dev
	pecl install mongodb
	echo "extension=mongodb.so" > /etc/php/7.0/cli/conf.d/30-mongodb.ini
	curl -sS https://getcomposer.org/installer | php
	php composer.phar install --no-plugins --no-scripts
fi

if [ "$RELEASE" == "trusty" ]; then
	echo "Installing from trusty which used PHP 5.5..."
	apt-get -y install php5-cli php5-dev php-pear libssl-dev libmongo-client-dev php5-curl
	pecl install mongodb
	echo "extension=mongodb.so" > /etc/php5/cli/conf.d/30-mongodb.ini
	curl -sS https://getcomposer.org/installer | php
	php composer.phar install --no-plugins --no-scripts
fi

