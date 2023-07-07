use tui::buffer::Buffer;
use tui::layout::Rect;
use tui::widgets::{StatefulWidget, Tabs, Widget};

#[derive(Debug, Clone, Default)]
pub(crate) struct TabState {
    selected: usize,
}

impl TabState {
    pub(crate) fn selected(&self) -> usize {
        self.selected
    }

    pub(crate) fn select(&mut self, index: usize) {
        self.selected = index;
    }
}

#[derive(Debug, Clone)]
pub(crate) struct StatefulTabs<'a> {
    tabs: Tabs<'a>,
}

impl<'a> From<Tabs<'a>> for StatefulTabs<'a> {
    fn from(tabs: Tabs<'a>) -> Self {
        Self { tabs }
    }
}

impl<'a> StatefulWidget for StatefulTabs<'a> {
    type State = TabState;

    fn render(self, area: Rect, buf: &mut Buffer, state: &mut Self::State) {
        self.tabs.select(state.selected).render(area, buf)
    }
}
