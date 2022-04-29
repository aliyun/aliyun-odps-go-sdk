package tunnel_test

import (
	"fmt"
	"log"
	"time"

	"github.com/aliyun/aliyun-odps-go-sdk/arrow/array"
	"github.com/aliyun/aliyun-odps-go-sdk/arrow/memory"
	tunnel2 "github.com/aliyun/aliyun-odps-go-sdk/odps/tunnel"
	"github.com/pkg/errors"
)

func Example_tunnel_upload_arrow() {
	tunnelIns.SetHttpTimeout(10 * time.Second)

	session, err := tunnelIns.CreateUploadSession(
		ProjectName,
		"sale_detail",
		tunnel2.SessionCfg.WithPartitionKey("sale_date='202111',region='hangzhou'"),
		// tunnel.SessionCfg.WithSnappyFramedCompressor(),
		// tunnel.SessionCfg.WithDeflateCompressor(tunnel.DeflateLevel.DefaultCompression),
		tunnel2.SessionCfg.WithDefaultDeflateCompressor(),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}
	schema := session.ArrowSchema()

	type SaleDetailData struct {
		ShopNames  []string
		CustomIDs  []string
		totalPrice []float64
	}

	rawData := []SaleDetailData{
		{
			[]string{"sun", "moon", "earth"},
			[]string{"fixed_start1", "satellite1", "planet3"},
			[]float64{10000.032, 200.00, 1500.232},
		},
		{
			[]string{"mars", "venus"},
			[]string{"planet4", "planet2"},
			[]float64{1000.1, 1232.2},
		},
		{
			[]string{"songjiang", "wusong"},
			[]string{"liangshan1", "liangshan2"},
			[]float64{100.13, 232.2},
		},
	}

	blockIds := make([]int, len(rawData))

	writeBlock := func(blockId int, data SaleDetailData) error {
		recordWriter, err := session.OpenRecordArrowWriter(blockId)
		if err != nil {
			return errors.WithStack(err)
		}

		pool := memory.NewGoAllocator()
		recordBuilder := array.NewRecordBuilder(pool, schema)
		defer recordBuilder.Release()

		for i, field := range schema.Fields() {
			fieldBuilder := recordBuilder.Field(i)

			switch field.Name {
			case "shop_name":
				builder := fieldBuilder.(*array.StringBuilder)
				builder.AppendValues(data.ShopNames, nil)
			case "customer_id":
				builder := fieldBuilder.(*array.StringBuilder)
				builder.AppendValues(data.CustomIDs, nil)
			case "total_price":
				builder := fieldBuilder.(*array.Float64Builder)
				builder.AppendValues(data.totalPrice, nil)
			}
		}

		record := recordBuilder.NewRecord()
		defer record.Release()

		err = recordWriter.WriteArrowRecord(record)
		if err != nil {
			return errors.WithStack(err)
		}

		return errors.WithStack(recordWriter.Close())
	}

	wait := make(chan error, len(rawData))

	for i, n := 0, len(rawData); i < n; i++ {
		i := i
		blockIds[i] = i

		go func() {
			err := writeBlock(i, rawData[i])

			wait <- err
		}()
	}

	for i, n := 0, len(rawData); i < n; i++ {
		e := <-wait
		if e != nil {
			log.Fatalf("%+v", err)
			return
		}
	}

	err = session.Commit(blockIds)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func Example_tunnel_download_arrow_simple() {
	session, err := tunnelIns.CreateDownloadSession(
		"test_new_console_gcc",
		// "upload_sample_arrow",
		"has_struct",
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordArrowReader(0, 2, nil)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}

func Example_tunnel_download_arrow_with_partition() {
	session, err := tunnelIns.CreateDownloadSession(
		"test_new_console_gcc",
		"sale_detail",
		tunnel2.SessionCfg.WithPartitionKey("sale_date='202111',region='hangzhou'"),
	)
	if err != nil {
		log.Fatalf("%+v", err)
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordArrowReader(
		0, 1000,
		[]string{"shop_name", "total_price"})

	if err != nil {
		log.Fatalf("%+v", err)
	}

	n := 0
	reader.Iterator(func(rec array.Record, err error) {
		if err != nil {
			log.Fatalf("%+v", err)
		}

		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	})

	err = reader.Close()
	if err != nil {
		log.Fatalf("%+v", err)
	}

	// Output:
}
