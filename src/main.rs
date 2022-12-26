
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;

#[derive(Debug)]
struct StatKV<T> {
    pub key: String,
    pub val: T,
}

#[tokio::main]
async fn main() {
    println!("connecting...");
    let c = TcpStream::connect(("localhost", 11211)).await.unwrap();
    let (r, mut w) = c.into_split();
    let b = BufReader::new(r);
    let mut lines = b.lines();


    for i in 0..2 {
        println!("writing...");
        w.write_all("stats\r\n".as_bytes()).await.unwrap();
        let mut out = Vec::new();

        println!("reading {}...", i);
        loop {
            let l = lines.next_line().await.unwrap();
            if let Some(v) = l {
                if "END" == v {
                    for i in out.iter() {
                        println!("{:?}", i)
                    }
                    out.clear();
                    break;
                } else if v.starts_with("STAT ")  {
                    let parts: Vec<String> = v.split(' ').map(|v| v.to_string()).collect();
                    if parts.len() != 3 {
                        panic!("bad split: {}", v);
                    }

                    out.push(StatKV { key: parts[1].clone(), val: parts[2].clone()})
                } else {
                    panic!("bad line: {}", v);
                }
            }
        }
    }
}
