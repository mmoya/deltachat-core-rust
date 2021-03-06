""" Contact object. """

from . import props
from .cutil import from_dc_charpointer
from .capi import lib, ffi


class Contact(object):
    """ Delta-Chat Contact.

    You obtain instances of it through :class:`deltachat.account.Account`.
    """
    def __init__(self, account, id):
        self.account = account
        self.id = id

    def __eq__(self, other):
        return self.account._dc_context == other.account._dc_context and self.id == other.id

    def __ne__(self, other):
        return not (self == other)

    def __repr__(self):
        return "<Contact id={} addr={} dc_context={}>".format(self.id, self.addr, self.account._dc_context)

    @property
    def _dc_contact(self):
        return ffi.gc(
            lib.dc_get_contact(self.account._dc_context, self.id),
            lib.dc_contact_unref
        )

    @props.with_doc
    def addr(self):
        """ normalized e-mail address for this account. """
        return from_dc_charpointer(lib.dc_contact_get_addr(self._dc_contact))

    @props.with_doc
    def display_name(self):
        """ display name for this contact. """
        return from_dc_charpointer(lib.dc_contact_get_display_name(self._dc_contact))

    def is_blocked(self):
        """ Return True if the contact is blocked. """
        return lib.dc_contact_is_blocked(self._dc_contact)

    def is_verified(self):
        """ Return True if the contact is verified. """
        return lib.dc_contact_is_verified(self._dc_contact)

    def get_profile_image(self):
        """Get contact profile image.

        :returns: path to profile image, None if no profile image exists.
        """
        dc_res = lib.dc_contact_get_profile_image(self._dc_contact)
        if dc_res == ffi.NULL:
            return None
        return from_dc_charpointer(dc_res)

    def get_chat(self):
        """return 1:1 chat for this contact. """
        return self.account.create_chat_by_contact(self)
