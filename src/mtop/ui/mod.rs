use crate::client::Measurement;
use crate::queue::BlockingMeasurementQueue;
use crossterm::event::{self, Event, KeyCode};
use std::io;
use std::time::Duration;
use tui::layout::Alignment;
use tui::widgets::Gauge;
use tui::{
    backend::Backend,
    layout::{Constraint, Direction, Layout},
    style::{Color, Modifier, Style},
    text::{Span, Spans},
    widgets::{Block, Borders, Cell, Paragraph, Row, Table, TableState, Tabs, Wrap},
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
    let rects = Layout::default()
        .direction(Direction::Vertical)
        .constraints(
            [
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(10),
                Constraint::Percentage(60),
            ]
            .as_ref(),
        )
        .margin(1)
        .split(f.size());

    let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    let normal_style = Style::default().bg(Color::Blue);

    let values = app.values();

    let titles = app
        .hosts
        .iter()
        .map(|t| {
            let (first, rest) = t.split_at(1);
            Spans::from(vec![
                Span::styled(first, Style::default().fg(Color::Yellow)),
                Span::styled(rest, Style::default().fg(Color::Green)),
            ])
        })
        .collect();

    let tabs = Tabs::new(titles)
        .block(Block::default().borders(Borders::ALL).title("Hosts"))
        .select(app.index)
        .highlight_style(selected_style);

    f.render_widget(tabs, rects[0]);

    if values.measurements.len() > 0 {
        let bytes = memory_gauge(&values.measurements[app.index].measurement);
        f.render_widget(bytes, rects[1]);

        let connections = connections_gauge(&values.measurements[app.index].measurement);
        f.render_widget(connections, rects[2]);

        let hits = hits_gauge(&values.measurements[app.index].measurement);
        f.render_widget(hits, rects[3]);
    }

    let headers = app.headers();
    let header_cells = headers
        .into_iter()
        .map(|h| Cell::from(h).style(Style::default().fg(Color::Cyan)));

    let header = Row::new(header_cells)
        .style(normal_style)
        .height(1)
        .bottom_margin(1);

    let rows = values.measurements.iter().map(|m| {
        let cells = vec![
            Cell::from(m.hostname()),
            Cell::from(m.connections()),
            Cell::from(m.gets()),
            Cell::from(m.sets()),
            Cell::from(m.read()),
            Cell::from(m.write()),
            Cell::from(m.bytes()),
            Cell::from(m.items()),
            Cell::from(m.evictions()),
        ];

        Row::new(cells).bottom_margin(1)
    });

    let t = Table::new(rows)
        .header(header)
        .block(Block::default().borders(Borders::ALL).title("Memcached"))
        .highlight_style(selected_style)
        .widths(&[
            Constraint::Percentage(20),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
            Constraint::Percentage(10),
        ]);
    f.render_stateful_widget(t, rects[4], &mut app.state);
}

fn memory_gauge(m: &Measurement) -> Gauge {
    let used = m.bytes as f64 / m.max_bytes as f64;
    let label = format!("{:.1}%", used * 100.0);
    Gauge::default()
        .block(Block::default().title("Memory").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Magenta))
        .percent((used * 100.0) as u16)
        .label(label)
}

fn connections_gauge(m: &Measurement) -> Gauge {
    let used = m.curr_connections as f64 / m.max_connections as f64;
    let label = format!("{}/{}", m.curr_connections, m.max_connections);
    Gauge::default()
        .block(Block::default().title("Connections").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Yellow))
        .percent((used * 100.0) as u16)
        .label(label)
}

fn hits_gauge(m: &Measurement) -> Gauge {
    let total = m.get_flushed + m.get_expired + m.get_hits + m.get_misses;
    let ratio = if total == 0 {
        0.0
    } else {
        m.get_hits as f64 / total as f64
    };

    let label = format!("{:.1}%", ratio * 100.0);
    Gauge::default()
        .block(Block::default().title("Hit Ratio").borders(Borders::ALL))
        .gauge_style(Style::default().fg(Color::Blue))
        .percent((ratio * 100.0) as u16)
        .label(label)
}

pub struct App {
    state: TableState,
    queue: BlockingMeasurementQueue,
    hosts: Vec<String>,
    index: usize,
}

impl App {
    pub fn new(hosts: Vec<String>, queue: BlockingMeasurementQueue) -> Self {
        App {
            state: TableState::default(),
            index: 0,
            hosts,
            queue,

        }
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

    fn headers(&self) -> Vec<&'static str> {
        vec![
            "Host",
            "Connections",
            "Gets",
            "Sets",
            "Read",
            "Write",
            "Bytes",
            "Items",
            "Evictions",
        ]
    }

    fn values(&self) -> ApplicationValues {
        let mut measurements = Vec::new();

        for addr in &self.hosts {
            if let Some(m) = self.queue.read(addr) {
                measurements.push(MeasurementRow {
                    hostname: addr.to_owned(),
                    measurement: m,
                })
            }
        }

        ApplicationValues { measurements }
    }
}

#[derive(Debug)]
struct ApplicationValues {
    measurements: Vec<MeasurementRow>,
}

#[derive(Debug)]
struct MeasurementRow {
    hostname: String,
    measurement: Measurement,
}

impl MeasurementRow {
    fn hostname(&self) -> String {
        self.hostname.clone()
    }

    fn connections(&self) -> String {
        self.measurement.curr_connections.to_string()
    }

    fn gets(&self) -> String {
        self.measurement.cmd_get.to_string()
    }

    fn sets(&self) -> String {
        self.measurement.cmd_set.to_string()
    }

    fn read(&self) -> String {
        human_bytes(self.measurement.bytes_read)
    }

    fn write(&self) -> String {
        human_bytes(self.measurement.bytes_written)
    }

    fn bytes(&self) -> String {
        human_bytes(self.measurement.bytes)
    }
    fn items(&self) -> String {
        self.measurement.curr_items.to_string()
    }

    fn evictions(&self) -> String {
        self.measurement.evictions.to_string()
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

    return format!(
        "{:.1}{}",
        val as f64 / scales[index].factor,
        scales[index].suffix
    );
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
