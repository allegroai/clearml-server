import rapidjson


# Dump datetime in ISO format
# treat "native" datetime objects as UTC
DATETIME_MODE = rapidjson.DM_ISO8601 | rapidjson.DM_NAIVE_IS_UTC

dumps = rapidjson.Encoder(datetime_mode=DATETIME_MODE)
dumps_notascii = rapidjson.Encoder(datetime_mode=DATETIME_MODE, ensure_ascii=False)
loads = rapidjson.Decoder(datetime_mode=DATETIME_MODE)
