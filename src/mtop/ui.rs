use crate::queue::{BlockingMeasurementQueue, MeasurementDelta};
use crossterm::event::{self, Event, KeyCode};
use std::io;
use std::time::Duration;
use tui::widgets::Gauge;
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, Tabs},
    Frame, Terminal,
};

pub fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> io::Result<()> {
    loop {
        terminal.draw(|f| render_table(f, &mut app))?;

        if let Ok(available) = event::poll(Duration::from_secs(1)) {
            if available {
                if let Event::Key(key) = event::read()? {
                    match key.code {
                        KeyCode::Char('q') => return Ok(()),
                        KeyCode::Right => app.next(),
                        KeyCode::Left => app.previous(),
                        _ => {}
                    }
                }
            }
        }
    }
}

fn render_table<B>(f: &mut Frame<B>, app: &mut App)
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
            .margin(1)
            .split(f.size());
        (chunks[0], chunks[1])
    };

    let host = app.current_host().unwrap_or("[unknown]".to_owned());
    let host_block = Block::default().title(host).borders(Borders::ALL);
    let inner_host_area = host_block.inner(host_area);
    f.render_widget(host_block, host_area);

    let (gauge_area_1, gauge_area_2, gauge_area_3) = {
        let chunks = Layout::default()
            .direction(Direction::Vertical)
            .constraints([
                Constraint::Length(5), // gauges 1
                Constraint::Length(5), // gauges 2
                Constraint::Length(5), // gauges 3
            ])
            .split(inner_host_area);
        (chunks[0], chunks[1], chunks[2])
    };

    let gauge_areas_1 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([
            Constraint::Percentage(33),
            Constraint::Percentage(33),
            Constraint::Percentage(33),
        ])
        .split(gauge_area_1);

    let gauge_areas_2 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(gauge_area_2);

    let gauge_areas_3 = Layout::default()
        .direction(Direction::Horizontal)
        .constraints([Constraint::Percentage(50), Constraint::Percentage(50)])
        .split(gauge_area_3);

    let tabs = host_tabs(app.hosts(), app.selected());
    f.render_widget(tabs, tab_area);

    if let Some(delta) = app.current_delta() {
        let bytes = memory_gauge(&delta);
        f.render_widget(bytes, gauge_areas_1[0]);

        let connections = connections_gauge(&delta);
        f.render_widget(connections, gauge_areas_1[1]);

        let hits = hits_gauge(&delta);
        f.render_widget(hits, gauge_areas_1[2]);

        let gets = gets_gauge(&delta);
        f.render_widget(gets, gauge_areas_2[0]);

        let sets = sets_gauge(&delta);
        f.render_widget(sets, gauge_areas_2[1]);

        let items = items_gauge(&delta);
        f.render_widget(items, gauge_areas_3[0]);

        let evictions = evictions_gauge(&delta);
        f.render_widget(evictions, gauge_areas_3[1]);
    }
}

fn host_tabs(hosts: &Vec<String>, index: usize) -> Tabs {
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

fn memory_gauge(m: &MeasurementDelta) -> Gauge {
    let used = m.current.bytes as f64 / m.current.max_bytes as f64;
    let label = format!("{}/{}", human_bytes(m.current.bytes), human_bytes(m.current.max_bytes));
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
        .gauge_style(Style::default().fg(Color::Red))
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
        .gauge_style(Style::default().fg(Color::Green))
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

pub struct App {
    queue: BlockingMeasurementQueue,
    hosts: Vec<String>,
    index: usize,
}

impl App {
    pub fn new(hosts: Vec<String>, queue: BlockingMeasurementQueue) -> Self {
        App { index: 0, hosts, queue }
    }

    pub fn next(&mut self) {
        self.index = (self.index + 1) % self.hosts.len();
    }

    pub fn previous(&mut self) {
        if self.index > 0 {
            self.index -= 1;
        } else {
            self.index = self.hosts.len() - 1;
        }
    }

    pub fn selected(&self) -> usize {
        self.index
    }

    pub fn hosts(&self) -> &Vec<String> {
        &self.hosts
    }

    pub fn current_host(&self) -> Option<String> {
        self.hosts.get(self.index).map(|h| h.clone())
    }

    pub fn current_delta(&self) -> Option<MeasurementDelta> {
        self.current_host().and_then(|h| self.queue.read_delta(&h))
    }
}

struct Scale {
    factor: f64,
    suffix: &'static str,
}

fn human_bytes(val: u64) -> String {
    let scales = vec![
        Scale {
            factor: 1024_f64.powi(0),
            suffix: "",
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
    ];

    if val == 0 {
        return val.to_string();
    }

    let l = (val as f64).log(1024.0).floor();
    let index = l as usize;

    return format!("{:.1}{}", val as f64 / scales[index].factor, scales[index].suffix);
}

#[cfg(test)]
mod test {
    use crate::ui::human_bytes;

    #[test]
    fn test_human_bytes() {
        let v = human_bytes(1024);
        println!("VAL: {}", v);

        let v = human_bytes(1024 * 5 + 378371);
        println!("VAL: {}", v);
    }
}
