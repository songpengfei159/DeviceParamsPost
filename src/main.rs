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
use calamine::{open_workbook, Reader, Xlsx, DataType};
use std::collections::HashMap;
use chrono::{NaiveDate, NaiveDateTime};

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
    card_en: String,
    card_st: String,
    validity_period: Option<String>,
}

// 定义数据结构
#[derive(Debug)]
struct SimCardInfo {
    msisdn: String,
    issue_time: Option<NaiveDateTime>,
    expiry_time: Option<NaiveDateTime>,
}
fn read_simcards_to_map(file_path: &str) -> Result<HashMap<String, SimCardInfo>, Box<dyn std::error::Error>> {
    let mut workbook: Xlsx<_> = open_workbook(file_path)?;
    let mut simcard_map = HashMap::new();

    // 假设数据在名为 "Sheet1" 的工作表中
    if let Some(Ok(range)) = workbook.worksheet_range("Sheet1") {
        // 遍历每一行数据 (跳过可能的标题行)
        for (i, row) in range.rows().enumerate() {
            // 跳过标题行（假设第一行是标题）
            if i == 0 { continue; }

            // 确保行有足够的数据列（至少4列）
            if row.len() < 4 {
                eprintln!("第 {} 行数据列数不足，跳过", i + 1);
                continue;
            }

            // 解析 MSISDN (作为键)
            let msisdn = match &row[0] {
                DataType::String(s) => s.clone(),
                DataType::Int(n) => n.to_string(),
                DataType::Float(f) => f.to_string(),
                _ => {
                    eprintln!("第 {} 行 MSISDN 格式异常，跳过", i + 1);
                    continue;
                }
            };

            // 解析 ICCID
            let iccid = match &row[1] {
                DataType::String(s) => s.clone(),
                DataType::Int(n) => n.to_string(),
                DataType::Float(f) => f.to_string(),
                _ => {
                    eprintln!("第 {} 行 ICCID 格式异常，跳过", i + 1);
                    continue;
                }
            };

            // 解析发卡时间（可选字段）
            let issue_time = parse_datetime(&row[2]);

            // 解析卡到期时间（可选字段）
            let expiry_time = parse_datetime(&row[3]);

            // 插入到 HashMap
            simcard_map.insert(iccid , SimCardInfo {
                msisdn ,
                issue_time,
                expiry_time,
            });
        }
    }

    Ok(simcard_map)
}
// 辅助函数：将 DataType 解析为 NaiveDateTime
fn parse_datetime(data: &DataType) -> Option<NaiveDateTime> {
    match data {
        DataType::DateTime(dt) => {
            // calamine 的 DateTime 类型是 f64 (OLE自动化日期)
            // 需要转换为 chrono 的 NaiveDateTime
            // 这里简单地将浮点数转换为字符串，实际使用时需要根据Excel日期格式进行转换
            Some(NaiveDateTime::from_timestamp(
                (dt * 86400.0 - 2209161600.0) as i64,
                0
            ))
        },
        DataType::String(s) => {
            // 尝试解析完整的日期时间
            if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Some(dt);
            }

            // 尝试解析仅日期格式（没有时间部分）
            if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
                // 将 NaiveDate 转换为 NaiveDateTime（时间设为 00:00:00）
                return Some(date.and_hms_opt(0, 0, 0).unwrap());
            }

            // 尝试其他常见的日期格式
            let date_formats = [
                "%Y/%m/%d",
                "%Y.%m.%d",
                "%Y年%m月%d日",
                "%d/%m/%Y",
                "%m/%d/%Y",
            ];

            for fmt in &date_formats {
                if let Ok(date) = NaiveDate::parse_from_str(s, fmt) {
                    return Some(date.and_hms_opt(0, 0, 0).unwrap());
                }
            }

            None
        },
        DataType::Empty => None,
        _ => None,
    }
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


async fn query_and_send_to_redis(card: &HashMap<String, SimCardInfo>, report_inertval: &mut HashMap<i32, NaiveDateTime>) -> redis::RedisResult<()> {
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
    let mut card_en = String::from("未知");
    let mut card_st = String::from("未知");
    for (CardID, battery, phonenu,TimeStamps) in results {
        // 组装数据
        let request_id = Uuid::new_v4().to_string();
        // 获取当前时间戳（毫秒）
        let report_time = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards")
            .as_millis() as i64;
        let mut validity_period:Option<String> = None;
        if let Some(info) = card.get(&phonenu.clone()) {
            println!("MSISDN: 8613800138000");
            card_st= info.issue_time.unwrap().clone().to_string();
            card_en= info.expiry_time.unwrap().clone().to_string();
            // 计算 今天到 card_en 的天数差,并且如果今天只汇报一次
            if(!report_inertval.contains_key(&CardID)){
                let now = chrono::Local::now().naive_local();
                let days_diff = (info.expiry_time.unwrap() - now).num_days();
                validity_period = Option::from(days_diff.to_string());
                report_inertval.insert(CardID, now);
            }
        }
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
                card_en: card_en.clone(),
                card_st: card_st.clone(),
                validity_period,
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
    let mut interval = time::interval(Duration::from_secs(30 * 60));
    let file_path = "./森赛尔-电话卡.xlsx"; // 你的Excel文件路径
    let simcard_map = read_simcards_to_map(file_path).expect("Failed to read SIM card data");

    // 打印结果
    println!("共读取 {} 条SIM卡记录", simcard_map.len());
    let mut report_inertval: HashMap<i32, NaiveDateTime> = HashMap::new();
    let mut count = 0;
    loop {
        interval.tick().await; // 等待 5 分钟
        // 没循环 48次 清空一次 report_inertval
        count += 1;
        if count >= 48 {
            report_inertval.clear();
            count = 0;
        }
        if let Err(e) = query_and_send_to_redis(&simcard_map, &mut report_inertval).await {
            eprintln!("Error occurred: {:?}", e);
        }
    }
}
