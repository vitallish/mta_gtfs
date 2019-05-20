FROM mysql

#RUN mkdir /usr/src/db
#WORKDIR /usr/src/db


#ENV MYSQL_DATABASE mta_trains
#ENV MYSQL_RANDOM_ROOT_PASSWORD yes
#ENV MYSQL_USER coolguy
#ENV MYSQL_PASSWORD coolpass

COPY model_generation_script.sql /docker-entrypoint-initdb.d
COPY my.cnf /etc/mysql/conf.d
#RUN mysql -u $MYSQL_USER -p$MYSQL_PASSWORD < model_generation_script.sql
