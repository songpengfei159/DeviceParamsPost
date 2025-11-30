use std::thread::sleep;
use std::time::{SystemTime, UNIX_EPOCH};
use mysql::Pool;
// use mysql::*;
use mysql::prelude::*;
use redis::{Client, Commands};
use serde::{Serialize};
use std::fs::File;
use tokio::time::{self, Duration};
use uuid::Uuid;
use std::io::{BufRead, BufReader};


#[derive(Serialize)]
struct DeviceData {
    id: String,
    reportTime: i64,
    deviceId: i32,
    tenantId: i32,
    serverId: Option<i32>,
    requestId: String,
    method: String,
    params: Params,
    data: Option<String>,
    code: Option<String>,
    msg: Option<String>,
}

#[derive(Serialize)]
struct Params {
    battery: String,
    phone: String,
    last_time: String,
}

// 从文件读取每一行，解析成 i64，返回 Vec<i64>
fn read_ids_from_file(path: &str) -> std::io::Result<Vec<i64>> {
    let f = File::open(path)?;
    let reader = BufReader::new(f);
    let mut ids = Vec::new();
    for line in reader.lines() {
        let s = line?.trim().to_string();
        if s.is_empty() {
            continue;
        }

        match s.parse::<i64>() {
            Ok(v) => ids.push(v),
            Err(_) => eprintln!("skip invalid id: sss{}ss", s),
        }
    }
    Ok(ids)
}


async fn query_and_send_to_redis() -> redis::RedisResult<()> {
    // 在您的 async 函数中替换原注释处的代码示例：
    // 读取文件，拼接占位符，构建 params 并执行带占位符的查询
    let id_list = match read_ids_from_file("./163_device") {
        Ok(v) if !v.is_empty() => v,
        Ok(_) => {
            println!("no ids found in file");
            Vec::new()
        }
        Err(e) => {
            eprintln!("failed to read ids file: {:?}", e);
            Vec::new()
        }
    };
    // 读取文件 163_device.csv 每一行 拼接成 in 语句

    let placeholders = id_list.iter().map(|id| id.to_string()).collect::<Vec<_>>().join(",");
    let sql = format!(
        "SELECT CardID, UseStatus, phonenu, TimeStamps FROM MedBoxDevice WHERE CardId IN ({})",
        placeholders
    );
    println!("start querying mysql: {}", sql);
    // 连接 MySQL
    println!("begin connect mysql");
    let pool = Pool::new("mysql://cdcasplus:cdcas_passwd123@192.168.9.109:3306/medboxdb").unwrap();
    let mut conn = pool.get_conn().unwrap();
    println!("connect mysql success");
    // 执行 SQL 查询
    let results: Vec<(i32, i32, String,String)> = conn.query(sql).unwrap();
    println!("device size: {:?}", results.len());
    // 连接 Redis
    let client = Client::open("redis://39.106.149.139/").unwrap();
    let mut con = client.get_connection()?;
    // 发送到 Redis Stream
    let stream_name = "iot_device_message"; // 可以根据需要修改 stream 名称

    for (CardID, battery, phonenu,TimeStamps) in results {
        // 组装数据
        let request_id = Uuid::new_v4().to_string();
        // 获取当前时间戳（毫秒）
        let report_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        let device_data = DeviceData {
            id: request_id.clone(),
            reportTime: report_time,
            deviceId: CardID,
            tenantId: 163,
            serverId: None,
            requestId: request_id.clone(),
            method: "thing.property.post".to_string(),
            params: Params {
                battery: battery.to_string(),
                phone: phonenu,
                last_time: TimeStamps,
            },
            data: None,
            code: None,
            msg: None,
        };

        // 转换成 JSON
        let json_data = serde_json::to_string(&device_data).unwrap();


        let _: () = con.xadd(stream_name, "*", &[("data", json_data)])?;

        println!("Data sent to Redis stream");
        tokio::time::sleep(Duration::from_micros(2500)).await;
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    // 定时任务：每 5 分钟执行一次
    let mut interval = time::interval(Duration::from_secs(15 * 60));

    loop {
        interval.tick().await; // 等待 5 分钟

        if let Err(e) = query_and_send_to_redis().await {
            eprintln!("Error occurred: {:?}", e);
        }
    }
}
