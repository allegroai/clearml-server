import rapidjson


# Dump datetime in ISO format
# treat "native" datetime objects as UTC
DATETIME_MODE = rapidjson.DM_ISO8601 | rapidjson.DM_NAIVE_IS_UTC

dumps = rapidjson.Encoder(datetime_mode=DATETIME_MODE)
loads = rapidjson.Decoder(datetime_mode=DATETIME_MODE)
