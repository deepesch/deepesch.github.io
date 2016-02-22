#!/bin/bash

timestamp=(`date +%Y-%m-%d'T'%H_%M_%S`)

# We're going to use the 'tez' execution engine instead of 'mapreduce'
exec_engine="tez"

######

pig -f toptopics_byusers_daily.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_toptopics_byusers'${timestamp}
pig -f toptopics_byusers_hourly.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_toptopics_byusers'${timestamp}

pig -f toptopics_bysize_daily.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_toptopics_bysize'${timestamp}
pig -f toptopics_bysize_hourly.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_toptopics_bysize'${timestamp}

pig -f toptopics_daily.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_toptopics'${timestamp}
pig -f toptopics_hourly.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_toptopics'${timestamp}

pig -f topics_dailyedits.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_topicedits_'${timestamp}
pig -f topics_hourlyedits.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_topicedits_'${timestamp}

pig -f users_dailyedits.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_useredits_'${timestamp}
pig -f users_hourlyedits.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_useredits_'${timestamp}

pig -f daily_uniquetopics-users.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/daily_uniquetopics_'${timestamp}
pig -f hourly_uniquetopics-users.pig -x ${exec_engine} -m 'pig_config.ini' -p output_folder='/hourly_uniquetopics_'${timestamp}



