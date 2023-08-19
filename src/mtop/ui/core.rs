use crate::client::SlabItem;
use crate::queue::{BlockingStatsQueue, StatsDelta};
use crate::ui::compat::{StatefulTabs, TabState};
use crossterm::event::{self, Event, KeyCode, KeyModifiers};
use crossterm::terminal::{self, EnterAlternateScreen, LeaveAlternateScreen};
use std::collections::HashMap;
use std::time::Duration;
use std::{io, panic};
use tui::backend::Backend;
use tui::layout::{Constraint, Direction, Layout, Rect};
use tui::style::{Color, Modifier, Style};
use tui::text::{Span, Spans};
use tui::widgets::{Block, Borders, Cell, Gauge, Row, Table, TableState, Tabs};
use tui::{backend::CrosstermBackend, Frame, Terminal};

const DRAW_INTERVAL: Duration = Duration::from_secs(1);

/// Disable text output and enable drawing of a UI on standard out.
pub fn initialize_terminal() -> io::Result<Terminal<CrosstermBackend<io::Stdout>>> {
    terminal::enable_raw_mode()?;
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, EnterAlternateScreen)?;
    Terminal::new(CrosstermBackend::new(stdout))
}

/// Re-enable text output on standard out
pub fn reset_terminal() -> io::Result<()> {
    let mut stdout = io::stdout();
    crossterm::execute!(stdout, LeaveAlternateScreen)?;
    terminal::disable_raw_mode()
}

/// Replace the existing panic handler with one that re-enables text output on
/// standard out before running the default panic handler.
pub fn install_panic_handler() {
    let original_hook = panic::take_hook();
    panic::set_hook(Box::new(move |p| {
        reset_terminal().unwrap();
        original_hook(p);
    }));
}

/// Draw the state of `app` on `terminal` until the user exits. Drawing happens
/// every second unless there is user input, in which case it happens immediately.
pub fn run<B>(terminal: &mut Terminal<B>, mut app: Application) -> io::Result<()>
where
    B: Backend,
{
    loop {
        terminal.draw(|f| render(f, &mut app))?;

        if event::poll(DRAW_INTERVAL)? {
            if let Event::Key(key) = event::read()? {
                let ctrl = key.modifiers.intersects(KeyModifiers::CONTROL);

                match key.code {
                    KeyCode::Char('q') => return Ok(()),
                    KeyCode::Char('c') if ctrl => return Ok(()),
                    KeyCode::Char('m') => app.toggle_mode(),
                    KeyCode::Right | KeyCode::Char('l') => app.next_host(),
                    KeyCode::Left | KeyCode::Char('h') => app.prev_host(),
                    KeyCode::Up | KeyCode::Char('k') => app.prev_row(),
                    KeyCode::Down | KeyCode::Char('j') => app.next_row(),
                    _ => {}
                }
            }
        }
    }
}

/// Draw the current state of `app` on the given frame `f`
fn render<B>(f: &mut Frame<B>, app: &mut Application)
where
    B: Backend,
{
    let host = app.host();
    let hosts = app.hosts();
    let inner_host_area = render_host_area(f, host, hosts, app.state.tabs());

    if let Some(delta) = app.read() {
        match app.state.mode() {
            Mode::Default => render_stats_gauges(f, inner_host_area, &delta),
            Mode::Slabs => render_slabs_table(f, inner_host_area, &delta, app.state.table()),
        }
    }
}

fn render_host_area<B>(f: &mut Frame<B>, host: String, hosts: Vec<String>, state: &mut TabState) -> Rect
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

    let tabs = host_tabs(&hosts);
    f.render_stateful_widget(tabs, tab_area, state);

    let host_block = Block::default().title(host).borders(Borders::ALL);
    let inner_host_area = host_block.inner(host_area);
    f.render_widget(host_block, host_area);

    inner_host_area
}

fn render_stats_gauges<B>(f: &mut Frame<B>, area: Rect, delta: &StatsDelta)
where
    B: Backend,
{
    let units = UnitFormatter::new();
    // Split up the host area into three rows. These will be further split
    // into 3 or 4 sections horizontally
    let (gauge_row_1, gauge_row_2, gauge_row_3) = {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Percentage(33),
                Constraint::Percentage(33),
                Constraint::Percentage(33),
            ])
            .split(area);

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

    let bytes = memory_gauge(delta, &units);
    f.render_widget(bytes, gauge_row_1[0]);

    let connections = connections_gauge(delta);
    f.render_widget(connections, gauge_row_1[1]);

    let hits = hits_gauge(delta);
    f.render_widget(hits, gauge_row_1[2]);

    let gets = gets_gauge(delta);
    f.render_widget(gets, gauge_row_2[0]);

    let sets = sets_gauge(delta);
    f.render_widget(sets, gauge_row_2[1]);

    let evictions = evictions_gauge(delta);
    f.render_widget(evictions, gauge_row_2[2]);

    let items = items_gauge(delta);
    f.render_widget(items, gauge_row_2[3]);

    let bytes_read = bytes_read_gauge(delta, &units);
    f.render_widget(bytes_read, gauge_row_3[0]);

    let bytes_written = bytes_written_gauge(delta, &units);
    f.render_widget(bytes_written, gauge_row_3[1]);

    let user_cpu = user_cpu_gauge(delta);
    f.render_widget(user_cpu, gauge_row_3[2]);

    let system_cpu = system_cpu_gauge(delta);
    f.render_widget(system_cpu, gauge_row_3[3]);
}

fn render_slabs_table<B>(f: &mut Frame<B>, area: Rect, delta: &StatsDelta, state: &mut TableState)
where
    B: Backend,
{
    let units = UnitFormatter::new();
    let header_style = Style::default().bg(Color::Blue);
    let selected_style = Style::default().bg(Color::Red).fg(Color::LightYellow);
    let header = slab_table_header(header_style);
    let rows = slab_table_rows(delta, &units);

    let t = Table::new(rows)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Slabs"))
        .highlight_style(selected_style)
        .widths(&[
            Constraint::Percentage(5),  // ID
            Constraint::Percentage(8),  // size
            Constraint::Percentage(8),  // pages
            Constraint::Percentage(13), // items
            Constraint::Percentage(15), // memory
            Constraint::Percentage(13), // max age
            Constraint::Percentage(17), // unfetched
            Constraint::Percentage(17), // expired
        ]);

    f.render_stateful_widget(t, area, state);
}

fn host_tabs(hosts: &[String]) -> StatefulTabs {
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

    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("Hosts"))
        .highlight_style(selected);

    StatefulTabs::from(tabs)
}

fn slab_table_header<'a>(style: Style) -> Row<'a> {
    Row::new(
        [
            "ID",
            "size",
            "pages",
            "items",
            "memory",
            "max age",
            "evicted\nunused",
            "expired\nunused",
        ]
        .into_iter()
        .map(Cell::from),
    )
    .style(style)
    .height(2)
    .bottom_margin(1)
}

fn slab_table_rows<'a>(delta: &StatsDelta, units: &UnitFormatter) -> Vec<Row<'a>> {
    // Convert SlabItems to a map of SlabItem indexed by the slab ID because once there are
    // no items in a slab class, Memcached doesn't return anything for it from `stats items`.
    let items: HashMap<u64, &SlabItem> = delta.current.items.iter().map(|i| (i.id, i)).collect();
    let mut rows = Vec::with_capacity(delta.current.slabs.len());

    for slab in delta.current.slabs.iter() {
        let (max_age, evicted, expired) = if let Some(i) = items.get(&slab.id) {
            (
                units.seconds(i.age),
                format!("{}", i.evicted_unfetched),
                format!("{}", i.expired_unfetched),
            )
        } else {
            ("n/a".to_owned(), "n/a".to_owned(), "n/a".to_owned())
        };

        let used = slab.used_chunks * slab.chunk_size;
        rows.push(
            Row::new([
                Cell::from(format!("{}", slab.id)),
                Cell::from(format!("{}b", slab.chunk_size)),
                Cell::from(format!("{}", slab.total_pages)),
                Cell::from(format!("{}", slab.used_chunks)),
                Cell::from(units.bytes(used)),
                Cell::from(max_age),
                Cell::from(evicted),
                Cell::from(expired),
            ])
            .height(1),
        )
    }

    rows
}

fn memory_gauge<'a>(m: &'a StatsDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let ratio = (m.current.stats.bytes as f64 / m.current.stats.max_bytes as f64).min(1.0);
    let label = format!(
        "{}/{}",
        units.bytes(m.current.stats.bytes),
        units.bytes(m.current.stats.max_bytes)
    );
    Gauge::default()
        .block(Block::default().title("Memory").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Magenta))
        .ratio(ratio)
        .label(label)
}

fn connections_gauge(m: &StatsDelta) -> Gauge {
    let ratio = (m.current.stats.curr_connections as f64 / m.current.stats.max_connections as f64).min(1.0);
    let label = format!(
        "{}/{}",
        m.current.stats.curr_connections, m.current.stats.max_connections
    );
    Gauge::default()
        .block(Block::default().title("Connections").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Yellow))
        .ratio(ratio)
        .label(label)
}

fn hits_gauge(m: &StatsDelta) -> Gauge {
    let total = (m.current.stats.get_flushed
        + m.current.stats.get_expired
        + m.current.stats.get_hits
        + m.current.stats.get_misses)
        - (m.previous.stats.get_flushed
            + m.previous.stats.get_expired
            + m.previous.stats.get_hits
            + m.previous.stats.get_misses);
    let hits = m.current.stats.get_hits - m.previous.stats.get_hits;
    let ratio = (if total == 0 { 0.0 } else { hits as f64 / total as f64 }).min(1.0);

    let label = format!("{:.1}%", ratio * 100.0);
    Gauge::default()
        .block(Block::default().title("Hit Ratio").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Blue))
        .ratio(ratio)
        .label(label)
}

fn gets_gauge(m: &StatsDelta) -> Gauge {
    let diff = (m.current.stats.cmd_get - m.previous.stats.cmd_get) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Gets").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightGreen))
        .percent(0)
        .label(label)
}

fn sets_gauge(m: &StatsDelta) -> Gauge {
    let diff = (m.current.stats.cmd_set - m.previous.stats.cmd_set) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Sets").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Cyan))
        .percent(0)
        .label(label)
}

fn evictions_gauge(m: &StatsDelta) -> Gauge {
    let diff = (m.current.stats.evictions - m.previous.stats.evictions) / m.seconds;
    let label = format!("{}/s", diff);
    Gauge::default()
        .block(Block::default().title("Evictions").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Red))
        .percent(0)
        .label(label)
}

fn items_gauge(m: &StatsDelta) -> Gauge {
    let label = format!("{}", m.current.stats.curr_items);
    Gauge::default()
        .block(Block::default().title("Items").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Yellow))
        .percent(0)
        .label(label)
}

fn bytes_read_gauge<'a>(m: &'a StatsDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let diff = (m.current.stats.bytes_read - m.previous.stats.bytes_read) / m.seconds;
    let label = format!("{}/s", units.bytes(diff));
    Gauge::default()
        .block(Block::default().title("Bytes rx").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightMagenta))
        .percent(0)
        .label(label)
}

fn bytes_written_gauge<'a>(m: &'a StatsDelta, units: &'a UnitFormatter) -> Gauge<'a> {
    let diff = (m.current.stats.bytes_written - m.previous.stats.bytes_written) / m.seconds;
    let label = format!("{}/s", units.bytes(diff));
    Gauge::default()
        .block(Block::default().title("Bytes tx").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightBlue))
        .percent(0)
        .label(label)
}

fn user_cpu_gauge(m: &StatsDelta) -> Gauge {
    let diff = ((m.current.stats.rusage_user - m.previous.stats.rusage_user) / m.seconds as f64) * 100.0;
    let label = format!("{:.1}%", diff);
    Gauge::default()
        .block(Block::default().title("User CPU").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightCyan))
        .percent(0)
        .label(label)
}

fn system_cpu_gauge(m: &StatsDelta) -> Gauge {
    let diff = ((m.current.stats.rusage_system - m.previous.stats.rusage_system) / m.seconds as f64) * 100.0;
    let label = format!("{:.1}%", diff);
    Gauge::default()
        .block(Block::default().title("System CPU").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::LightRed))
        .percent(0)
        .label(label)
}
/// Holds the current state of the application such as stats data and currently
/// selected host to render in the UI.
#[derive(Debug)]
pub struct Application {
    stats: BlockingStatsQueue,
    state: State,
    hosts: Vec<String>,
    num_rows: usize,
}

impl Application {
    pub fn new(hosts: &[String], stats: BlockingStatsQueue) -> Self {
        Application {
            stats,
            state: State::new(hosts.len()),
            hosts: Vec::from(hosts),
            num_rows: 0,
        }
    }

    /// Select the next row in the slabs table
    fn next_row(&mut self) {
        self.state.next_row(self.num_rows);
    }

    /// Select the previous row in the slabs table
    fn prev_row(&mut self) {
        self.state.prev_row(self.num_rows);
    }

    /// Select the next host, as ordered by `hosts`
    fn next_host(&mut self) {
        self.state.next_host();
    }

    /// Select the previous host, as ordered by `hosts`
    fn prev_host(&mut self) {
        self.state.prev_host();
    }

    /// Toggle between different UI modes
    fn toggle_mode(&mut self) {
        self.state.set_mode(if self.state.mode() == Mode::Default {
            Mode::Slabs
        } else {
            Mode::Default
        })
    }

    /// Get the hostnames of all hosts we have stats for
    fn hosts(&self) -> Vec<String> {
        self.hosts.clone()
    }

    /// Get the hostname of the currently selected host
    fn host(&self) -> String {
        self.hosts[self.state.selected()].clone()
    }

    /// Get most recent and least recent stats for the currently selected host
    fn read(&mut self) -> Option<StatsDelta> {
        let delta = self.stats.read_delta(&self.hosts[self.state.selected()]);
        if let Some(d) = &delta {
            self.num_rows = d.current.slabs.len();
        }

        delta
    }
}

/// Mode toggles between various views in the UI.
///
/// `Default` is gauges arranged into several rows. `Slabs` shows per-slab based
/// information in a table.
#[derive(Debug, Clone, Copy, Eq, PartialEq)]
#[repr(u8)]
enum Mode {
    Default,
    Slabs,
}

/// Mutable state needed to render the mtop UI.
///
/// Number of hosts must be at least `1` is cannot change while mtop is running. Tab
/// state controls which host metrics are displayed for. Each table state maintains
/// the selected row of a slabs table.
#[derive(Debug)]
struct State {
    num_hosts: usize,
    mode: Mode,
    tabs: TabState,
    tables: Vec<TableState>,
}

impl State {
    fn new(num_hosts: usize) -> Self {
        Self {
            num_hosts,
            mode: Mode::Default,
            tabs: TabState::default(),
            tables: (0..num_hosts).map(|_| TableState::default()).collect(),
        }
    }

    fn next_host(&mut self) {
        self.tabs.select((self.tabs.selected() + 1) % self.num_hosts);
    }

    fn prev_host(&mut self) {
        self.tabs.select(if self.tabs.selected() > 0 {
            self.tabs.selected() - 1
        } else {
            self.num_hosts - 1
        });
    }

    fn next_row(&mut self, total: usize) {
        if self.mode != Mode::Slabs {
            return;
        }

        let table = &mut self.tables[self.tabs.selected()];
        let selected = if let Some(current) = table.selected() {
            if total == 0 || current >= total - 1 {
                0
            } else {
                current + 1
            }
        } else {
            0
        };

        table.select(Some(selected));
    }

    fn prev_row(&mut self, total: usize) {
        if self.mode != Mode::Slabs {
            return;
        }

        let table = &mut self.tables[self.tabs.selected()];
        let selected = if let Some(current) = table.selected() {
            if total == 0 {
                0
            } else if current == 0 {
                total - 1
            } else {
                current - 1
            }
        } else {
            0
        };

        table.select(Some(selected));
    }

    fn selected(&self) -> usize {
        self.tabs.selected()
    }

    fn mode(&self) -> Mode {
        self.mode
    }

    fn set_mode(&mut self, mode: Mode) {
        self.mode = mode;
    }

    fn tabs(&mut self) -> &mut TabState {
        &mut self.tabs
    }

    fn table(&mut self) -> &mut TableState {
        &mut self.tables[self.tabs.selected()]
    }
}

#[derive(Debug)]
struct BytesScale {
    factor: f64,
    suffix: &'static str,
}

/// Formatter for displaying human readable versions of various metrics.
#[derive(Debug)]
struct UnitFormatter {
    bytes_scale: Vec<BytesScale>,
}

impl UnitFormatter {
    fn new() -> Self {
        UnitFormatter {
            bytes_scale: vec![
                BytesScale {
                    factor: 1024_f64.powi(0),
                    suffix: "b",
                },
                BytesScale {
                    factor: 1024_f64.powi(1),
                    suffix: "k",
                },
                BytesScale {
                    factor: 1024_f64.powi(2),
                    suffix: "M",
                },
                BytesScale {
                    factor: 1024_f64.powi(3),
                    suffix: "G",
                },
                BytesScale {
                    factor: 1024_f64.powi(4),
                    suffix: "T",
                },
                BytesScale {
                    factor: 1024_f64.powi(5),
                    suffix: "P",
                },
                BytesScale {
                    factor: 1024_f64.powi(6),
                    suffix: "E",
                },
                BytesScale {
                    factor: 1024_f64.powi(7),
                    suffix: "Z",
                },
            ],
        }
    }

    fn seconds(&self, mut secs: u64) -> String {
        let hours = secs / 3600;
        if hours > 0 {
            secs %= 3600;
        }

        let mins = secs / 60;
        if mins > 0 {
            secs %= 60;
        }

        format!("{:0>2}:{:0>2}:{:0>2}", hours, mins, secs)
    }

    fn bytes(&self, val: u64) -> String {
        if val == 0 {
            return val.to_string();
        }

        let l = (val as f64).log(1024.0).floor();
        let index = l as usize;

        self.bytes_scale
            .get(index)
            .map(|s| format!("{:.1}{}", val as f64 / s.factor, s.suffix))
            .unwrap_or_else(|| val.to_string())
    }
}
