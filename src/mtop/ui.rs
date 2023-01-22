use crate::client::Measurement;
use crossterm::event::{self, Event, KeyCode};
use std::collections::HashMap;
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

pub struct App {
    state: TableState,
    items: Vec<MeasurementRow>,
}

impl App {
    pub fn new(measurements: Vec<Measurement>) -> Self {
        App {
            state: TableState::default(),
            items: measurements
                .into_iter()
                .map(|m| MeasurementRow::from(m))
                .collect(),
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
