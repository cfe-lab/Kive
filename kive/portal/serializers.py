from rest_framework import serializers

import portal.models


class StagedFileSerializer(serializers.ModelSerializer):
    user = serializers.SlugRelatedField(
        slug_field="username",
        read_only=True
    )

    class Meta:
        model = portal.models.StagedFile
        fields = (
            "pk",
            "uploaded_file",
            "user",
            "date_uploaded"
        )

    def create(self, validated_data):
        staged_file = portal.models.StagedFile(
            user=self.context["request"].user,
            **validated_data
        )

        staged_file.save()

        return staged_file