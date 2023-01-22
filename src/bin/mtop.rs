use clap::Parser;
use crossterm::event::{self, DisableMouseCapture, EnableMouseCapture, Event, KeyCode};
use crossterm::execute;
use crossterm::terminal::{
    disable_raw_mode, enable_raw_mode, EnterAlternateScreen, LeaveAlternateScreen,
};
use std::collections::{HashMap, VecDeque};
use std::error;
use std::io;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::Mutex;
use tokio::task;
use tracing::{Instrument, Level};
use tui::{
    backend::{Backend, CrosstermBackend},
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table, TableState},
    Frame, Terminal,
};
use mtop::client::{Measurement, StatReader, StatsCommand};

const DEFAULT_LOG_LEVEL: Level = Level::INFO;
const DEFAULT_STATS_INTERVAL_MS: u64 = 1000;
const NUM_MEASUREMENTS: usize = 3;

/// mtop: top for memcached
#[derive(Debug, Parser)]
#[clap(name = "mtop", version = clap::crate_version ! ())]
struct MtopApplication {
    /// Logging verbosity. Allowed values are 'trace', 'debug', 'info', 'warn', and 'error'
    /// (case insensitive)
    #[clap(long, default_value_t = DEFAULT_LOG_LEVEL)]
    log_level: Level,

    /// Memcached hosts to connect to in the form 'hostname:port'. Must be specified at least
    /// once and may be used multiple times (separated by spaces).
    #[clap(required = true)]
    hosts: Vec<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn error::Error + Send + Sync>> {
    let opts = MtopApplication::parse();

    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_max_level(opts.log_level)
            .finish(),
    )
    .expect("failed to set tracing subscriber");

    // TODO: Need a queue per hostname
    let queue = Arc::new(Mutex::new(VecDeque::with_capacity(NUM_MEASUREMENTS)));
    let queue_ref = queue.clone();

    task::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
        loop {
            let _ = interval.tick().await;

            for addr in opts.hosts.iter() {
                // TODO: need to somehow handle this failure in main thread
                //tracing::info!(message = "connecting", address = ?addr);
                let c = TcpStream::connect(addr).await.unwrap();
                let (r, w) = c.into_split();
                let mut rw = StatReader::new(r, w);

                match rw
                    .read_stats(StatsCommand::Default)
                    .instrument(tracing::span!(Level::DEBUG, "read_stats"))
                    .await
                {
                    Ok(v) => {
                        let mut q = queue_ref.lock().await;
                        q.push_back(v);
                        if q.len() > NUM_MEASUREMENTS {
                            q.pop_front();
                        }
                    }
                    Err(e) => tracing::warn!(message = "failed to fetch stats", "err" = %e),
                }
            }
        }
    });

    let mut m: Option<Measurement>;
    let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
    loop {
        let _ = interval.tick().await;
        let queue_ref = queue.clone();

        let mut q = queue_ref.lock().await;
        //tracing::info!(message = "queue entries", entries = q.len());

        if let Some(e) = q.pop_front() {
            m = Measurement::try_from(&e).ok();
            break;
            //tracing::info!(message = "raw stats", stats = ?e);
            //tracing::info!(message = "parsed stats", stats = ?m)
        }
    }

    task::spawn_blocking(move || {
        enable_raw_mode().unwrap();
        let mut stdout = io::stdout();
        execute!(stdout, EnterAlternateScreen, EnableMouseCapture).unwrap();
        let backend = CrosstermBackend::new(stdout);
        let mut terminal = Terminal::new(backend).unwrap();

        let app = App::new(m.unwrap());
        let res = run_app(&mut terminal, app);

        disable_raw_mode().unwrap();
        execute!(
            terminal.backend_mut(),
            LeaveAlternateScreen,
            DisableMouseCapture
        )
        .unwrap();
        terminal.show_cursor().unwrap();
    });

    //  let mut interval = tokio::time::interval(Duration::from_millis(DEFAULT_STATS_INTERVAL_MS));
    //  loop {
    //      let _ = interval.tick().await;
    //      let queue_ref = queue.clone();
    //
    //      let mut q = queue_ref.lock().await;
    //      //tracing::info!(message = "queue entries", entries = q.len());
    //
    //      if let Some(e) = q.pop_front() {
    //          let m = Measurement::try_from(&e).unwrap();
    //          //tracing::info!(message = "raw stats", stats = ?e);
    //          //tracing::info!(message = "parsed stats", stats = ?m)
    //      }
    // }

    Ok(())
}

fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> io::Result<()> {
    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        if let Event::Key(key) = event::read()? {
            match key.code {
                KeyCode::Char('q') => return Ok(()),
                KeyCode::Down => app.next(),
                KeyCode::Up => app.previous(),
                _ => {}
            }
        }
    }
}

fn ui<B: Backend>(f: &mut Frame<B>, app: &mut App) {
    let rects = Layout::default()
        .constraints([Constraint::Percentage(100)].as_ref())
        .margin(5)
        .split(f.size());

    let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    let normal_style = Style::default().bg(Color::Blue);
    let header_cells = [
        "Connections",
        "Gets",
        "Sets",
        "Read",
        "Write",
        "Bytes",
        "Items",
    ]
    .iter()
    .map(|h| Cell::from(*h).style(Style::default().fg(Color::Red)));
    let header = Row::new(header_cells)
        .style(normal_style)
        .height(1)
        .bottom_margin(1);
    let rows = app.items.iter().map(|item| {
        let height = item
            .values
            .iter()
            .map(|(_, content)| content.chars().filter(|c| *c == '\n').count())
            .max()
            .unwrap_or(0)
            + 1;
        let cells = item.values.iter().map(|(_, c)| Cell::from(c.clone()));
        Row::new(cells).height(height as u16).bottom_margin(1)
    });
    let t = Table::new(rows)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Table"))
        .highlight_style(selected_style)
        .highlight_symbol(">> ")
        .widths(&[
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(15),
            Constraint::Percentage(10),
        ]);
    f.render_stateful_widget(t, rects[0], &mut app.state);
}

struct App {
    state: TableState,
    items: Vec<MeasurementRow>,
}

impl App {
    fn new(m: Measurement) -> Self {
        App {
            state: TableState::default(),
            items: vec![MeasurementRow::from(m)],
        }
    }

    fn next(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i >= self.items.len() - 1 {
                    0
                } else {
                    i + 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }

    fn previous(&mut self) {
        let i = match self.state.selected() {
            Some(i) => {
                if i == 0 {
                    self.items.len() - 1
                } else {
                    i - 1
                }
            }
            None => 0,
        };
        self.state.select(Some(i));
    }
}

struct MeasurementRow {
    hostname: String,
    measurement: Measurement,
    values: HashMap<&'static str, String>,
}

impl MeasurementRow {
    fn from(m: Measurement) -> Self {
        let mut map = HashMap::new();
        map.insert("Connections", m.total_connections.to_string());
        map.insert("Gets", m.cmd_get.to_string());
        map.insert("Sets", m.cmd_set.to_string());
        map.insert("Read", m.bytes_read.to_string());
        map.insert("Write", m.bytes_written.to_string());
        map.insert("Bytes", m.bytes.to_string());
        map.insert("Items", m.curr_items.to_string());

        MeasurementRow {
            hostname: "localhost:11211".to_owned(),
            measurement: m,
            values: map,
        }
    }
}
