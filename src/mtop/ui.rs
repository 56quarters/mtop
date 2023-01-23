use crate::client::Measurement;
use crate::queue::BlockingMeasurementQueue;
use crossterm::event::{self, Event, KeyCode};
use std::io;
use std::time::Duration;
use tui::{
    backend::Backend,
    layout::{Constraint, Layout},
    style::{Color, Modifier, Style},
    widgets::{Block, Borders, Cell, Row, Table, TableState},
    Frame, Terminal,
};

pub fn run_app<B: Backend>(terminal: &mut Terminal<B>, mut app: App) -> io::Result<()> {
    loop {
        terminal.draw(|f| ui(f, &mut app))?;

        if let Ok(available) = event::poll(Duration::from_secs(1)) {
            if available {
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
    }
}

fn ui<B: Backend>(f: &mut Frame<B>, app: &mut App) {
    let rects = Layout::default()
        .constraints([Constraint::Percentage(100)].as_ref())
        .margin(5)
        .split(f.size());

    let selected_style = Style::default().add_modifier(Modifier::REVERSED);
    let normal_style = Style::default().bg(Color::Blue);

    let headers = app.headers();
    let header_cells = headers
        .into_iter()
        .map(|h| Cell::from(h).style(Style::default().fg(Color::Cyan)));

    let header = Row::new(header_cells)
        .style(normal_style)
        .height(1)
        .bottom_margin(1);

    let values = app.values();
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
        .highlight_symbol(">> ")
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
    f.render_stateful_widget(t, rects[0], &mut app.state);
}

pub struct App {
    state: TableState,
    queue: BlockingMeasurementQueue,
    hosts: Vec<String>,
}

impl App {
    pub fn new(hosts: Vec<String>, queue: BlockingMeasurementQueue) -> Self {
        App {
            state: TableState::default(),
            hosts,
            queue,
        }
    }

    fn next(&mut self) {
        // let i = match self.state.selected() {
        //     Some(i) => {
        //         if i >= self.items.len() - 1 {
        //             0
        //         } else {
        //             i + 1
        //         }
        //     }
        //     None => 0,
        // };
        // self.state.select(Some(i));
    }

    fn previous(&mut self) {
        // let i = match self.state.selected() {
        //     Some(i) => {
        //         if i == 0 {
        //             self.items.len() - 1
        //         } else {
        //             i - 1
        //         }
        //     }
        //     None => 0,
        // };
        // self.state.select(Some(i));
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
        self.measurement.bytes_read.to_string()
    }

    fn write(&self) -> String {
        self.measurement.bytes_written.to_string()
    }

    fn bytes(&self) -> String {
        self.measurement.bytes.to_string()
    }
    fn items(&self) -> String {
        self.measurement.curr_items.to_string()
    }

    fn evictions(&self) -> String {
        self.measurement.evictions.to_string()
    }
}
