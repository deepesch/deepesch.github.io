import kafka 

KAFKA_BROKER = "172.31.6.79:9092"

channel_list = ["en.wikipedia"]
#channel_list.append("mytopic")

# this value is used to identify the consumer group to keep track of offsets
group = "mygroup"

mykafka = kafka.KafkaClient(KAFKA_BROKER)


channel = channel_list[0]

consumer = (kafka.SimpleConsumer(mykafka, group, channel))
	
# this is an offset rewinding function
#consumer.seek(0, 0)

try:
	messages = consumer.get_messages(count=2000, block=False)

	# it looks like we get a TypeError Exception if no new messages exist
	for message in messages:
		print "****"
		print (message)
		print "Message length: %s" % (len(message))

		print "* Offset *"
		print  message[0]
	
		# get the value back out of the kafka consumer's fetched message
		print "* Message *"
		print message[1].value
		print len(message[1].value)
		print "\n"

	print "\n"
	print "Received %s total messages" % (len(messages))

except KeyboardInterrupt:
	pass

except TypeError as e:
	print "Got TypeError"	
	print e	

except Exception as e:
	print "Some other exception!"
	print e

consumer.commit()

mykafka.close()
