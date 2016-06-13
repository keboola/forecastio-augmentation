FROM keboola/base-php70
MAINTAINER Jakub Matejka <jakub@keboola.com>
ENV DEBIAN_FRONTEND noninteractive

RUN composer self-update

ADD . /code

RUN cd /code && composer install --prefer-dist --no-interaction

WORKDIR /code

CMD php ./src/run.php --data=/data