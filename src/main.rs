use std::io::{self, Error as IOError, ErrorKind};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::net::TcpStream;

#[derive(Debug)]
struct RawStat {
    pub key: String,
    pub val: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    println!("connecting...");
    let c = TcpStream::connect(("localhost", 11211)).await.unwrap();
    let (r, mut w) = c.into_split();
    let b = BufReader::new(r);
    let mut lines = b.lines();

    for i in 0..2 {
        println!("writing...");
        w.write_all("stats\r\n".as_bytes()).await.unwrap();

        println!("reading {}...", i);
        let stats = read_stats(&mut lines).await?;
        for kv in stats {
            println!("{:?}", kv);
        }
    }

    Ok(())
}

async fn read_stats<T>(lines: &mut Lines<T>) -> io::Result<Vec<RawStat>>
where
    T: AsyncBufRead + Unpin,
{
    let mut out = Vec::new();

    loop {
        let line = lines.next_line().await?;
        match line.as_deref() {
            Some("END") | None => break,
            Some(v) => {
                let mut parts = v.split(' ');
                match (parts.next(), parts.next(), parts.next()) {
                    (Some("STAT"), Some(key), Some(val)) => out.push(RawStat {
                        key: key.to_string(),
                        val: val.to_string(),
                    }),
                    _ => {
                        // TODO: Create our own error type with useful context
                        return Err(IOError::from(ErrorKind::InvalidData));
                    }
                }
            }
        }
    }

    Ok(out)
}
