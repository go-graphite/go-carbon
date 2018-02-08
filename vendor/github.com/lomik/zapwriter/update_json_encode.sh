#!/bin/sh

cp _vendor/src/go.uber.org/zap/zapcore/json_encoder.go json_encoder.go

sed -i.bak 's/zapcore/zapwriter/' json_encoder.go 
sed -i.bak 's/"go.uber.org\/zap\/internal\/bufferpool"/"go.uber.org\/zap\/zapcore"/' json_encoder.go
sed -i.bak 's/*EncoderConfig/*zapcore.EncoderConfig/' json_encoder.go
sed -i.bak 's/cfg EncoderConfig/cfg zapcore.EncoderConfig/' json_encoder.go
sed -i.bak 's/ Encoder/ zapcore.Encoder/' json_encoder.go
sed -i.bak 's/ ArrayMarshaler/ zapcore.ArrayMarshaler/' json_encoder.go
sed -i.bak 's/ ObjectMarshaler/ zapcore.ObjectMarshaler/' json_encoder.go
sed -i.bak 's/ Entry/ zapcore.Entry/' json_encoder.go
sed -i.bak 's/ \[\]Field/ []zapcore.Field/' json_encoder.go

rm -f json_encoder.go.bak
