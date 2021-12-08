package tunnel_test

import (
	"fmt"
	odps "github.com/aliyun/aliyun-odps-go-sdk"
	"github.com/aliyun/aliyun-odps-go-sdk/tunnel"
	"github.com/fetchadd/arrow/array"
	"github.com/fetchadd/arrow/memory"
	"os"
	"time"
)

var tunnelIns tunnel.Tunnel
var odpsIns *odps.Odps

func init() {
	accessId := os.Getenv("tunnel_odps_accessId")
	accessKey := os.Getenv("tunnel_odps_accessKey")
	odpsEndpoint := os.Getenv("odps_endpoint")
	tunnelEndpoint := os.Getenv("tunnel_odps_endpoint")

	account := odps.NewAliyunAccount(accessId, accessKey)
	odpsIns = odps.NewOdps(account, odpsEndpoint)
	tunnelIns = tunnel.NewTunnel(odpsIns, tunnelEndpoint)
}

func Example_tunnel_upload_arrow() {
	tunnelIns.SetHttpTimeout(10 * time.Second)

	session, err := tunnelIns.CreateUploadSession(
		"test_new_console_gcc",
		"sale_detail",
		tunnel.SessionCfg.WithPartitionKey("sale_date='202111',region='hangzhou'"),
		//tunnel.SessionCfg.WithSnappyFramedCompressor(),
		//tunnel.SessionCfg.WithDeflateCompressor(tunnel.DeflateLevel.DefaultCompression),
		tunnel.SessionCfg.WithDefaultDeflateCompressor(),
	)
	if err != nil {
		println(err.Error())
		return
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
		recordWriter, err := session.OpenRecordWriter(blockId)
		if err != nil {
			return err
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
			return err
		}

		return recordWriter.Close()
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
			println(e.Error())
			return
		}
	}

	err = session.Commit(blockIds)
	if err != nil {
		println(err.Error())
		return
	}

	// Output:
}

func Example_tunnel_download_arrow_simple() {
	session, err := tunnelIns.CreateDownloadSession(
		"test_new_console_gcc",
		"upload_sample_arrow",
	)
	if err != nil {
		println(err.Error())
		return
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordReader(0, 2, []string{"payload"})
	if err != nil {
		println(err.Error())
		return
	}

	n := 0
	for rec := range reader.Iterator() {
		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	}

	err = reader.Close()
	if err != nil {
		println(err)
	}

	// Output:
}

func Example_tunnel_download_arrow_with_partition() {
	session, err := tunnelIns.CreateDownloadSession(
		"test_new_console_gcc",
		"sale_detail",
	)
	if err != nil {
		println(err.Error())
		return
	}

	recordCount := session.RecordCount()
	println(fmt.Sprintf("record count is %d", recordCount))

	reader, err := session.OpenRecordReader(
		0, 1000,
		[]string{"shop_name", "total_price"})

	if err != nil {
		println(err.Error())
		return
	}

	n := 0
	for rec := range reader.Iterator() {
		for i, col := range rec.Columns() {
			println(fmt.Sprintf("rec[%d][%d]: %v", n, i, col))
		}

		rec.Release()
		n++
	}

	err = reader.Close()
	if err != nil {
		println(err)
	}

	// Output:
}
