use crate::queue::{BlockingMeasurementQueue, MeasurementDelta};
use crossterm::event::{self, Event, KeyCode};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use std::time::Duration;
use std::{io, panic};
use tui::backend::Backend;
use tui::layout::{Constraint, Direction, Layout};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Gauge, Tabs};
use tui::{backend::CrosstermBackend, Frame, Terminal};

const DRAW_INTERVAL: Duration = Duration::from_secs(1);

pub fn initialize_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

pub fn reset_terminal() -> io::Result<()> {
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, LeaveAlternateScreen)?;
    terminal::disable_raw_mode()
}

pub fn install_panic_handler() {
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |p| {
        reset_terminal().unwrap();
        original_hook(p);
    }));
}

pub fn run_app<B>(terminal: &mut Terminal<B>, mut app: Application) -> io::Result<()>
where
    B: Backend,
{
    loop {
        terminal.draw(|f| render(f, &mut app))?;

        if event::poll(DRAW_INTERVAL)? {
            if let Event::Key(key) = event::read()? {
                match key.code {
                    KeyCode::Char('q') => return Ok(()),
                    KeyCode::Right | KeyCode::Char('l') => app.next(),
                    KeyCode::Left | KeyCode::Char('h') => app.previous(),
                    _ => {}
                }
            }
        }
    }
}

fn render<B>(f: &mut Frame<B>, app: &mut Application)
where
    B: Backend,
{
    let (tab_area, host_area) = {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(3),      // tabs
                Constraint::Percentage(90), // host info
            ])
            .split(f.size());
        (chunks[0], chunks[1])
    };

    let host = app.current_host().unwrap_or_else(|| "[unknown]".to_owned());
    let host_block = Block::default().title(host).borders(Borders::ALL);
    let inner_host_area = host_block.inner(host_area);
    f.render_widget(host_block, host_area);

    let (gauge_row_1, gauge_row_2, gauge_row_3) = {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(33),
                Constraint::Percentage(33),
                Constraint::Percentage(33),
            ])
            .split(inner_host_area);

        let gauges_1 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(33),
                Constraint::Percentage(33),
                Constraint::Percentage(33),
            ])
            .split(chunks[0]);

        let gauges_2 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
            ])
            .split(chunks[1]);

        let gauges_3 = Layout::default()
            .direction(Direction::Horizontal)
            .constraints([
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
                Constraint::Percentage(25),
            ])
            .split(chunks[2]);

        (gauges_1, gauges_2, gauges_3)
    };

    let tabs = host_tabs(app.hosts(), app.selected_host());
    f.render_widget(tabs, tab_area);

    if let Some(delta) = app.current_delta() {
        let units = UnitFormatter::new();

        let bytes = memory_gauge(&delta, &units);
        f.render_widget(bytes, gauge_row_1[0]);

        let connections = connections_gauge(&delta);
        f.render_widget(connections, gauge_row_1[1]);

        let hits = hits_gauge(&delta);
        f.render_widget(hits, gauge_row_1[2]);

        let gets = gets_gauge(&delta);
        f.render_widget(gets, gauge_row_2[0]);

        let sets = sets_gauge(&delta);
        f.render_widget(sets, gauge_row_2[1]);

        let evictions = evictions_gauge(&delta);
        f.render_widget(evictions, gauge_row_2[2]);

        let items = items_gauge(&delta);
        f.render_widget(items, gauge_row_2[3]);

        let bytes_read = bytes_read_gauge(&delta, &units);
        f.render_widget(bytes_read, gauge_row_3[0]);

        let bytes_written = bytes_written_gauge(&delta, &units);
        f.render_widget(bytes_written, gauge_row_3[1]);

        let user_cpu = user_cpu_gauge(&delta);
        f.render_widget(user_cpu, gauge_row_3[2]);

        let system_cpu = system_cpu_gauge(&delta);
        f.render_widget(system_cpu, gauge_row_3[3])
    }
}

fn host_tabs(hosts: &[String], index: usize) -> Tabs {
    let selected = Style::default().add_modifier(Modifier::REVERSED);

    let titles = hosts
        .iter()
        .map(|t| {
            let (first, rest) = t.split_at(1);
            Spans::from(vec![
                Span::styled(first, Style::default().fg(Color::Cyan)),
                Span::styled(rest, Style::default()),
            ])
        })
        .collect();

    Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("Hosts"))
        .select(index)
        .highlight_style(selected)
}

fn memory_gauge<'a>(m: &'a MeasurementDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let used = m.current.bytes as f64 / m.current.max_bytes as f64;
    let label = format!("{}/{}", units.bytes(m.current.bytes), units.bytes(m.current.max_bytes));
    Gauge::default()
        .block(Block::default().title("Memory").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Magenta))
        .percent((used * 100.0) as u16)
        .label(label)
}

fn connections_gauge(m: &MeasurementDelta) -> Gauge {
    let used = m.current.curr_connections as f64 / m.current.max_connections as f64;
    let label = format!("{}/{}", m.current.curr_connections, m.current.max_connections);
    Gauge::default()
        .block(Block::default().title("Connections").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Yellow))
        .percent((used * 100.0) as u16)
        .label(label)
}

fn hits_gauge(m: &MeasurementDelta) -> Gauge {
    let total = (m.current.get_flushed + m.current.get_expired + m.current.get_hits + m.current.get_misses)
        - (m.previous.get_flushed + m.previous.get_expired + m.previous.get_hits + m.previous.get_misses);
    let hits = m.current.get_hits - m.previous.get_hits;
    let ratio = if total == 0 { 0.0 } else { hits as f64 / total as f64 };

    let label = format!("{:.1}%", ratio * 100.0);
    Gauge::default()
        .block(Block::default().title("Hit Ratio").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Blue))
        .percent((ratio * 100.0) as u16)
        .label(label)
}

fn gets_gauge(m: &MeasurementDelta) -> Gauge {
    let diff = (m.current.cmd_get - m.previous.cmd_get) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Gets").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightGreen))
        .percent(0)
        .label(label)
}

fn sets_gauge(m: &MeasurementDelta) -> Gauge {
    let diff = (m.current.cmd_set - m.previous.cmd_set) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Sets").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .percent(0)
        .label(label)
}

fn evictions_gauge(m: &MeasurementDelta) -> Gauge {
    let diff = (m.current.evictions - m.previous.evictions) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Evictions").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Red))
        .percent(0)
        .label(label)
}

fn items_gauge(m: &MeasurementDelta) -> Gauge {
    let label = format!("{}", m.current.curr_items);
    Gauge::default()
        .block(Block::default().title("Items").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Yellow))
        .percent(0)
        .label(label)
}

fn bytes_read_gauge<'a>(m: &'a MeasurementDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let diff = (m.current.bytes_read - m.previous.bytes_read) / m.seconds;
    let label = format!("{}/s", units.bytes(diff));
    Gauge::default()
        .block(Block::default().title("Bytes rx").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightMagenta))
        .percent(0)
        .label(label)
}

fn bytes_written_gauge<'a>(m: &'a MeasurementDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let diff = (m.current.bytes_written - m.previous.bytes_written) / m.seconds;
    let label = format!("{}/s", units.bytes(diff));
    Gauge::default()
        .block(Block::default().title("Bytes tx").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightBlue))
        .percent(0)
        .label(label)
}

fn user_cpu_gauge(m: &MeasurementDelta) -> Gauge {
    let diff = ((m.current.rusage_user - m.previous.rusage_user) / m.seconds as f64) * 100.0;
    let label = format!("{:.1}%", diff);
    Gauge::default()
        .block(Block::default().title("User CPU").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightCyan))
        .percent(0)
        .label(label)
}

fn system_cpu_gauge(m: &MeasurementDelta) -> Gauge {
    let diff = ((m.current.rusage_system - m.previous.rusage_system) / m.seconds as f64) * 100.0;
    let label = format!("{:.1}%", diff);
    Gauge::default()
        .block(Block::default().title("System CPU").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightRed))
        .percent(0)
        .label(label)
}

pub struct Application {
    measurements: BlockingMeasurementQueue,
    hosts: Vec<String>,
    selected_host: usize,
}

impl Application {
    pub fn new(hosts: &[String], measurements: BlockingMeasurementQueue) -> Self {
        Application {
            measurements,
            hosts: Vec::from(hosts),
            selected_host: 0,
        }
    }

    pub fn next(&mut self) {
        self.selected_host = (self.selected_host + 1) % self.hosts.len();
    }

    pub fn previous(&mut self) {
        if self.selected_host > 0 {
            self.selected_host -= 1;
        } else {
            self.selected_host = self.hosts.len() - 1;
        }
    }

    pub fn hosts(&self) -> &[String] {
        &self.hosts
    }

    pub fn selected_host(&self) -> usize {
        self.selected_host
    }

    pub fn current_host(&self) -> Option<String> {
        self.hosts.get(self.selected_host).cloned()
    }

    pub fn current_delta(&self) -> Option<MeasurementDelta> {
        self.current_host().and_then(|h| self.measurements.read_delta(&h))
    }
}

struct Scale {
    factor: f64,
    suffix: &'static str,
}

struct UnitFormatter {
    scales: Vec<Scale>,
}

impl UnitFormatter {
    fn new() -> Self {
        UnitFormatter {
            scales: vec![
                Scale {
                    factor: 1024_f64.powi(0),
                    suffix: "b",
                },
                Scale {
                    factor: 1024_f64.powi(1),
                    suffix: "k",
                },
                Scale {
                    factor: 1024_f64.powi(2),
                    suffix: "M",
                },
                Scale {
                    factor: 1024_f64.powi(3),
                    suffix: "G",
                },
                Scale {
                    factor: 1024_f64.powi(4),
                    suffix: "T",
                },
                Scale {
                    factor: 1024_f64.powi(5),
                    suffix: "P",
                },
                Scale {
                    factor: 1024_f64.powi(6),
                    suffix: "E",
                },
                Scale {
                    factor: 1024_f64.powi(7),
                    suffix: "Z",
                },
            ],
        }
    }

    fn bytes(&self, val: u64) -> String {
        if val == 0 {
            return val.to_string();
        }

        let l = (val as f64).log(1024.0).floor();
        let index = l as usize;

        self.scales
            .get(index)
            .map(|s| format!("{:.1}{}", val as f64 / s.factor, s.suffix))
            .unwrap_or_else(|| val.to_string())
    }
}
