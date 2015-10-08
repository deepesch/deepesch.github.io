from deck import Deck
from war_player import Player


class War(object):
    def __init__(self, human=True):
        self.human = human
        self.player1 = self.create_player("Player 1")
        self.player2 = self.create_player("Player 2")
        self.winner = None
        self.loser = None
        self.pot = []
        self.deal()

    def create_player(self, title):
        if self.human:
            name = raw_input("Enter %s's name: " % title)
        else:
            name = title
        return Player(name)

    def deal(self):
        deck = Deck()
        deck.shuffle()
        while not deck.isempty():
            self.player1.receive_card(deck.draw_card())
            self.player2.receive_card(deck.draw_card())

    def play_game(self):
        while self.winner is None:
            self.play_round()
        self.display_winner()

    def draw_card(self, player, other_player):
        card = player.play_card()
        if not card:
            self.winner = other_player.name
            self.loser = player.name
            return
        self.pot.append(card)
        return card

    def draw_cards(self, player, other_player, n):
        cards = []
        n = min(n, len(player.hand), len(other_player.hand))
        for i in xrange(n):
            card = self.draw_card(player, other_player)
            if not card:
                return
            cards.append(card)
        return cards

    def war(self):
        cards1 = self.draw_cards(self.player1, self.player2, 3)
        cards2 = self.draw_cards(self.player2, self.player1, 3)
        self.display_war(cards1, cards2)

    def play_round(self):
        self.pause()
        card1 = self.draw_card(self.player1, self.player2)
        card2 = self.draw_card(self.player2, self.player1)
        self.display_play(card1, card2)
        if not card1 or not card2:
            return
        if card1 == card2:
            self.war()
            self.play_round()
        elif card1 > card2:
            self.give_cards(self.player1)
        else:
            self.give_cards(self.player2)

    def give_cards(self, player):
        player.receive_cards(self.pot)
        self.display_receive(player)
        self.pot = []

    def pause(self):
        if self.human:
            raw_input("")

    def cards_to_str(self, cards):
        return " ".join(str(card) for card in cards)

    def display_play(self, card1, card2):
        if self.human:
            print "%s plays %s" % (self.player1.name, str(card1))
            print "%s plays %s" % (self.player2.name, str(card2))

    def display_receive(self, player):
        if self.human:
            self.pot.sort(reverse=True)
            pot_str = self.cards_to_str(self.pot)
            print "%s receives the cards: %s" % (player.name, pot_str)

    def display_war(self, cards1, cards2):
        if self.human:
            print "WAR!"
            print "%s plays %s" % (self.player1.name, self.cards_to_str(cards1))
            print "%s plays %s" % (self.player2.name, self.cards_to_str(cards2))

    def display_winner(self):
        if self.human:
            print "The winner is %s!!!" % self.winner


if __name__ == '__main__':
    game = War()
    game.play_game()