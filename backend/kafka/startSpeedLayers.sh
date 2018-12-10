# Pass weather speed layer jar path as the first argument
if [ -z "$1" ]
    then
        jarPath1="./speed_layer_weather_update/target/uber-speed_layer_weather_update-0.0.1-SNAPSHOT.jar"
    else
        jarPath1="$1"
fi
# Pass crime speed layer jar path as the first argument
if [ -z "$2" ]
    then
        jarPath2="./speed_layer_crime_update/target/uber-speed_layer_crime_update-0.0.1-SNAPSHOT.jar"
    else
        jarPath2="$2"
fi

nohup spark-submit --master local[2] --class StreamWeather $jarPath1 localhost:9092 &

nohup spark-submit --master local[2] --class StreamCrime $jarPath2 localhost:9092 &
