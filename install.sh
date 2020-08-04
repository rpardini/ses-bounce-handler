#! /bin/bash

# Horrible script for installation on Ubuntu machines

RELEASE=$(lsb_release -c -s)

if [ "$RELEASE" == "bionic" ]; then
	echo "Installing for bionic, which uses PHP 7 and has mongodb driver..."
	apt-get -y install php-mbstring php-cli php-mongodb php-curl php-xml
	curl -sS https://getcomposer.org/installer | php
	php composer.phar install --no-plugins --no-scripts
fi

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

# write a default cron entry, commented out

if [ ! -f /etc/cron.d/ses-bounce-handler ]; then
cat << EOD > /etc/cron.d/ses-bounce-handler
# /etc/cron.d/ses-bounce-handler - crontab fragment for running ses-bounce-handler.
# this contains authentication info, set it below before uncommenting

# */5 * * * * root /usr/bin/php /opt/ses-bounce-handler/ses-bounce-handler.php --region "REGION" --access "ACCESS_KEY" --secret "SECRET_KEY" --mailDomain "MAIL_DOMAIN" --mongoDbHost "MONGO_SERVER_IP" --cron
EOD
echo "**CONFIGURE SETTINGS AND UNCOMMENT IN /etc/cron.d/ses-bounce-handler**"
fi

echo "Enabling transport maps for transport_banned on postfix..."
touch /etc/postfix/transport_banned
postmap /etc/postfix/transport_banned
postconf -e 'transport_maps = hash:/etc/postfix/transport_banned'
service postfix reload

echo "Done! Don't forget to configure /etc/cron.d/ses-bounce-handler !"
