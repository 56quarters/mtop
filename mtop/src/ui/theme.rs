use mtop_client::MtopError;
use ratatui::style::palette::{material, tailwind};
use ratatui::style::Color;
use std::str::FromStr;

pub const MATERIAL: Theme = Theme {
    // general
    border: material::BLUE.c500,
    background: material::GRAY.c900,
    title: material::GRAY.c100,
    text: Color::Reset,

    // tabs
    tab_highlight: material::CYAN.c500,
    tab_selected: material::GRAY.c700,
    tab_scrollbar_arrows: material::YELLOW.c500,
    tab_scrollbar_track: material::BLUE.c500,
    tab_scrollbar_thumb: material::GREEN.c500,

    // gauges
    memory: material::PINK.c600,
    connections: material::ORANGE.c600,
    hits: material::PURPLE.c600,
    gets: material::GREEN.c500,
    sets: material::CYAN.c500,
    evictions: material::RED.c500,
    items: material::YELLOW.c500,
    bytes_rx: material::PINK.c500,
    bytes_tx: material::TEAL.c500,
    user_cpu: material::CYAN.c500,
    system_cpu: material::RED.c500,

    // table
    table_header: material::GRAY.c100,
    table_select_bg: material::RED.c500,
    table_select_fg: material::YELLOW.c300,
};

pub const TAILWIND: Theme = Theme {
    // general
    border: tailwind::BLUE.c500,
    background: tailwind::GRAY.c900,
    title: tailwind::GRAY.c100,
    text: Color::Reset,

    // tabs
    tab_highlight: tailwind::CYAN.c500,
    tab_selected: tailwind::GRAY.c700,
    tab_scrollbar_arrows: tailwind::YELLOW.c500,
    tab_scrollbar_track: tailwind::BLUE.c500,
    tab_scrollbar_thumb: tailwind::GREEN.c500,

    // gauges
    memory: tailwind::PINK.c600,
    connections: tailwind::ORANGE.c600,
    hits: tailwind::PURPLE.c600,
    gets: tailwind::GREEN.c500,
    sets: tailwind::CYAN.c500,
    evictions: tailwind::RED.c500,
    items: tailwind::YELLOW.c500,
    bytes_rx: tailwind::PINK.c500,
    bytes_tx: tailwind::TEAL.c500,
    user_cpu: tailwind::CYAN.c500,
    system_cpu: tailwind::RED.c500,

    // table
    table_header: tailwind::GRAY.c100,
    table_select_bg: tailwind::RED.c500,
    table_select_fg: tailwind::YELLOW.c300,
};

/// The collection of colors used in various places in the mtop UI.
#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Theme {
    // general
    pub border: Color,
    pub background: Color,
    pub title: Color,
    pub text: Color,

    // tabs
    pub tab_highlight: Color,
    pub tab_selected: Color,
    pub tab_scrollbar_arrows: Color,
    pub tab_scrollbar_track: Color,
    pub tab_scrollbar_thumb: Color,

    // gauges
    pub memory: Color,
    pub connections: Color,
    pub hits: Color,
    pub gets: Color,
    pub sets: Color,
    pub evictions: Color,
    pub items: Color,
    pub bytes_rx: Color,
    pub bytes_tx: Color,
    pub user_cpu: Color,
    pub system_cpu: Color,

    // table
    pub table_header: Color,
    pub table_select_bg: Color,
    pub table_select_fg: Color,
}

impl FromStr for Theme {
    type Err = MtopError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_lowercase();
        match s.as_str() {
            "tailwind" => Ok(TAILWIND),
            "material" => Ok(MATERIAL),
            _ => Err(MtopError::configuration(format!("invalid theme {}", s))),
        }
    }
}
