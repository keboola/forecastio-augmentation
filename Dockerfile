FROM keboola/base-php70
MAINTAINER Jakub Matejka <jakub@keboola.com>

WORKDIR /tmp

RUN yum -y --enablerepo=epel,remi,remi-php70 install \
		php-pdo_mysql \
		&& yum clean all

ADD . /code
WORKDIR /code

RUN composer install --no-interaction

ENTRYPOINT php ./src/run.php --data=/data