#
chmod +x submit.sh
chmod +x sbt/sbt

# package
./sbt/sbt assembly

# update env.sh with path to Spark home directory and Twitter application keys

# update submit.sh with your own twitter account


# to submit your streaming app. 
./submit 


