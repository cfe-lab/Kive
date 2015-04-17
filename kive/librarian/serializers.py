from librarian.models import SymbolicDataset
from rest_framework import serializers


class SymbolicDatasetSerializer(serializers.ModelSerializer):
    class Meta:
        model = SymbolicDataset
        fields = ('id', 'name')

